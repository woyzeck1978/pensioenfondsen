"""LLM-extractie van deelnemers / SFDR / EU taxonomy uit lokale jaarverslag PDFs.

Gebruikt een Ollama-instance op het Tailscale-netwerk (mistral-small) om
specifieke velden uit elke PDF te halen. Resultaten gaan eerst naar de
scratch-tabel `llm_extracted`; pas met --apply gaan ze (alleen NULL-cellen,
met sanity guards) naar funds.

Per PDF:
  1) Vind pagina's met keywords voor elk concept
  2) Bouw een 8-10 pagina context (max ~15k chars)
  3) Eén Ollama call met format=json
  4) Sanity-validate de waarden voordat ze de scratch-tabel raken

Run from scripts/document_parsing/:
    python3 llm_extract.py                      # process all 109 target funds
    python3 llm_extract.py --limit 5            # smoke test
    python3 llm_extract.py --apply              # only run apply step
    python3 llm_extract.py --funds 13,32,111    # comma list of ids
"""
import argparse
import glob
import json
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import date, datetime

import fitz  # pymupdf

OLLAMA = "http://100.71.107.24:11434"
MODEL = "mistral-small"
DB_PATH = "../../data/processed/pension_funds.db"
DIRS = ["../../data/annual_reports", "../../data/reports"]
LOG_DIR = "../../logs/llm_extract"

# Pages with these keywords are candidates per concept
KW = {
    "sfdr": re.compile(r"sfdr|duurzaam(?:heid)?|artikel\s*[689]\b", re.I),
    "deelnemers": re.compile(r"kerncijfers|aantal\s+deelnemers|deelnemers,\s+actief|gewezen\s+deelnemers|pensioengerechtig", re.I),
    "taxonomy": re.compile(r"taxonomie|taxonomy|sustainable\s+investment", re.I),
}

PROMPT = """Je krijgt fragmenten uit een Nederlands pensioenfonds-jaarverslag. \
Lever ALLEEN JSON terug — geen toelichting, geen markdown.

Schema (gebruik null waar niet expliciet vermeld):
{{
  "sfdr_article": 6 | 8 | 9 | null,
  "deelnemers_actief": int | null,
  "deelnemers_slapers": int | null,
  "deelnemers_gepensioneerd": int | null,
  "deelnemers_totaal": int | null,
  "eu_taxonomy_pct": number | null
}}

Regels:
- sfdr_article: alleen als het fonds zelf als artikel 6/8/9 SFDR-product wordt geclassificeerd
  (vaak: "valt onder artikel 8 van de SFDR", "Artikel 8 SFDR-product", "wordt aangemerkt als artikel 8").
  Algemene SFDR-verwijzingen (artikel 3, 4, 5) zijn NIET classificaties.
- Aantallen zonder duizendpunten, als integers. 10573 (niet 10.573, niet 10573.0)
- Jaartallen (2020-2030) zijn GEEN deelnemer-aantallen — sla over.
- Als er een 5-jaars tabel is, neem ALTIJD de meest recente kolom (meestal links of rechts; eerstgenoemd jaartal in de header).
- eu_taxonomy_pct: positief percentage 0-100. "3,5%" -> 3.5.

BRON:
{context}
"""


def build_inventory() -> dict[int, str]:
    """Map fund_id -> newest PDF path."""
    by_fund = defaultdict(list)
    for d in DIRS:
        for path in glob.glob(f"{d}/*.pdf"):
            base = os.path.basename(path)
            m = re.match(r"^(\d+)[_\s]", base)
            if not m:
                continue
            ym = re.search(r"(20\d{2})", base)
            by_fund[int(m.group(1))].append((int(ym.group(1)) if ym else 0, path))
    return {fid: sorted(items, reverse=True)[0][1] for fid, items in by_fund.items()}


def build_context(pdf_path: str, max_pages: int = 10) -> tuple[str, list[int]]:
    """Find pages matching any of the concept keywords, return concatenated context."""
    doc = fitz.open(pdf_path)
    candidates: dict[int, str] = {}
    for i, p in enumerate(doc):
        if i >= 250:
            break
        text = p.get_text()
        if any(rx.search(text) for rx in KW.values()):
            candidates[i + 1] = text
    doc.close()
    if not candidates:
        return "", []
    selected = sorted(candidates.items())[:max_pages]
    pages = [n for n, _ in selected]
    ctx = "\n\n--- BREAK ---\n\n".join(
        f"[page {n}]\n{t.strip()[:1500]}" for n, t in selected
    )
    return ctx, pages


def call_ollama(prompt: str, num_ctx: int = 12000, timeout: int = 600) -> tuple[dict, float]:
    body = json.dumps({
        "model": MODEL, "prompt": prompt, "stream": False, "format": "json",
        "options": {"num_ctx": num_ctx, "temperature": 0.1, "num_predict": 200},
    }).encode()
    req = urllib.request.Request(
        f"{OLLAMA}/api/generate", data=body,
        headers={"Content-Type": "application/json"},
    )
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        resp = json.load(r)
    return resp, time.time() - t0


def sanity_check(d: dict) -> dict:
    """Drop values that fail basic plausibility checks (return cleaned copy)."""
    out = dict(d)
    today_year = date.today().year

    # sfdr_article: must be 6, 8, or 9
    if out.get("sfdr_article") not in (6, 8, 9, None):
        out["sfdr_article"] = None

    for k in ("deelnemers_actief", "deelnemers_slapers",
              "deelnemers_gepensioneerd", "deelnemers_totaal"):
        v = out.get(k)
        if v is None:
            continue
        if not isinstance(v, int):
            try:
                v = int(v)
            except Exception:
                out[k] = None
                continue
        if v < 100:
            out[k] = None; continue
        if 1900 <= v <= today_year + 5:  # almost certainly a year
            out[k] = None; continue
        if v > 5_000_000:  # nothing in NL is bigger than ABP's ~3M
            out[k] = None; continue
        out[k] = v

    # Cross check: totaal ≈ sum
    tot = out.get("deelnemers_totaal")
    parts = [out.get(k) for k in ("deelnemers_actief", "deelnemers_slapers", "deelnemers_gepensioneerd") if out.get(k)]
    if tot and len(parts) >= 2:
        s = sum(parts)
        if abs(s - tot) / max(tot, 1) > 0.25:
            # mismatch — keep components, drop totaal (or trust components)
            out["deelnemers_totaal"] = None

    pct = out.get("eu_taxonomy_pct")
    if pct is not None:
        try:
            pct = float(pct)
        except Exception:
            out["eu_taxonomy_pct"] = None
        else:
            if pct < 0 or pct > 100:
                out["eu_taxonomy_pct"] = None
            else:
                out["eu_taxonomy_pct"] = round(pct, 2)
    return out


def ensure_schema(con: sqlite3.Connection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS llm_extracted (
            fund_id INTEGER PRIMARY KEY,
            pdf_path TEXT,
            sfdr_article INTEGER,
            deelnemers_actief INTEGER,
            deelnemers_slapers INTEGER,
            deelnemers_gepensioneerd INTEGER,
            deelnemers_totaal INTEGER,
            eu_taxonomy_pct REAL,
            raw_json TEXT,
            pages_used TEXT,
            latency_s REAL,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(fund_id) REFERENCES funds(id)
        )
    """)
    con.commit()


def select_targets(con: sqlite3.Connection, inv: dict[int, str],
                   funds_filter: list[int] | None, limit: int) -> list[int]:
    if funds_filter:
        return [fid for fid in funds_filter if fid in inv]
    # Any fund with at least one of the three gaps + a PDF
    rows = con.execute("""
        SELECT id FROM funds
        WHERE deelnemers_totaal IS NULL
           OR sfdr_article IS NULL
           OR eu_taxonomy_pct IS NULL
    """).fetchall()
    target = [r[0] for r in rows if r[0] in inv]
    if limit:
        target = target[:limit]
    return target


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--funds", type=str, default="", help="comma list of fund ids")
    ap.add_argument("--apply", action="store_true", help="only run the apply step")
    ap.add_argument("--resume", action="store_true",
                    help="skip funds already in llm_extracted")
    args = ap.parse_args()

    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    log_f = open(log_path, "a", buffering=1)

    def log(msg):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        print(line, flush=True)
        log_f.write(line + "\n")

    con = sqlite3.connect(DB_PATH)
    ensure_schema(con)

    if args.apply:
        log("APPLY-only mode")
        apply_to_funds(con, log)
        con.close()
        return

    inv = build_inventory()
    funds_filter = [int(x) for x in args.funds.split(",") if x.strip()] if args.funds else None
    targets = select_targets(con, inv, funds_filter, args.limit)
    if args.resume:
        done = {r[0] for r in con.execute("SELECT fund_id FROM llm_extracted").fetchall()}
        targets = [t for t in targets if t not in done]
    n = len(targets)
    log(f"start | model={MODEL} | targets={n} | log={log_path}")

    t_start = time.time()
    ok_sfdr = ok_deeln = ok_tax = errors = 0

    for i, fid in enumerate(targets, 1):
        pdf = inv[fid]
        try:
            ctx, pages = build_context(pdf)
            if not ctx:
                log(f"[{i}/{n}] fid={fid} {os.path.basename(pdf)[:50]}  SKIP no relevant pages")
                continue
            prompt = PROMPT.format(context=ctx)
            resp, dt = call_ollama(prompt)
            raw = resp.get("response", "")
            try:
                data = json.loads(raw)
            except Exception:
                log(f"[{i}/{n}] fid={fid} JSON-PARSE-ERR after {dt:.0f}s -- raw: {raw[:120]!r}")
                errors += 1
                continue
            clean = sanity_check(data)
            con.execute("""
                INSERT INTO llm_extracted (fund_id, pdf_path, sfdr_article,
                    deelnemers_actief, deelnemers_slapers, deelnemers_gepensioneerd,
                    deelnemers_totaal, eu_taxonomy_pct, raw_json, pages_used, latency_s)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(fund_id) DO UPDATE SET
                    pdf_path=excluded.pdf_path,
                    sfdr_article=excluded.sfdr_article,
                    deelnemers_actief=excluded.deelnemers_actief,
                    deelnemers_slapers=excluded.deelnemers_slapers,
                    deelnemers_gepensioneerd=excluded.deelnemers_gepensioneerd,
                    deelnemers_totaal=excluded.deelnemers_totaal,
                    eu_taxonomy_pct=excluded.eu_taxonomy_pct,
                    raw_json=excluded.raw_json,
                    pages_used=excluded.pages_used,
                    latency_s=excluded.latency_s,
                    extracted_at=CURRENT_TIMESTAMP
            """, (fid, pdf, clean.get("sfdr_article"),
                  clean.get("deelnemers_actief"), clean.get("deelnemers_slapers"),
                  clean.get("deelnemers_gepensioneerd"), clean.get("deelnemers_totaal"),
                  clean.get("eu_taxonomy_pct"), raw, json.dumps(pages), dt))
            con.commit()
            if clean.get("sfdr_article"): ok_sfdr += 1
            if clean.get("deelnemers_totaal") or clean.get("deelnemers_actief"): ok_deeln += 1
            if clean.get("eu_taxonomy_pct") is not None: ok_tax += 1
            log(f"[{i}/{n}] fid={fid:>3} {dt:>5.0f}s  "
                f"sfdr={clean.get('sfdr_article')} "
                f"actief={clean.get('deelnemers_actief')} "
                f"tax={clean.get('eu_taxonomy_pct')}")
        except urllib.error.URLError as e:
            log(f"[{i}/{n}] fid={fid} URL-ERR: {e}")
            errors += 1
        except Exception as e:
            log(f"[{i}/{n}] fid={fid} ERR: {e}")
            errors += 1

    elapsed = time.time() - t_start
    log(f"done | total {elapsed/60:.1f}min | sfdr_hits={ok_sfdr} deeln_hits={ok_deeln} tax_hits={ok_tax} errors={errors}")
    log_f.close()
    con.close()


def apply_to_funds(con, log):
    """Fill NULLs in funds from llm_extracted, with sanity guards."""
    new_sfdr = new_deeln_actief = new_deeln_slapers = new_deeln_gepens = new_deeln_totaal = new_tax = 0

    rows = con.execute("""
        SELECT fund_id, sfdr_article, deelnemers_actief, deelnemers_slapers,
               deelnemers_gepensioneerd, deelnemers_totaal, eu_taxonomy_pct
        FROM llm_extracted
    """).fetchall()
    for fid, sfdr, dact, dsla, dgep, dtot, tax in rows:
        if sfdr is not None:
            r = con.execute(
                "UPDATE funds SET sfdr_article = ? WHERE id = ? AND sfdr_article IS NULL",
                (sfdr, fid))
            new_sfdr += r.rowcount
        if dact is not None:
            r = con.execute(
                "UPDATE funds SET deelnemers_actief = ? WHERE id = ? AND deelnemers_actief IS NULL",
                (dact, fid))
            new_deeln_actief += r.rowcount
        if dsla is not None:
            r = con.execute(
                "UPDATE funds SET deelnemers_slapers = ? WHERE id = ? AND deelnemers_slapers IS NULL",
                (dsla, fid))
            new_deeln_slapers += r.rowcount
        if dgep is not None:
            r = con.execute(
                "UPDATE funds SET deelnemers_gepensioneerd = ? WHERE id = ? AND deelnemers_gepensioneerd IS NULL",
                (dgep, fid))
            new_deeln_gepens += r.rowcount
        if dtot is not None:
            r = con.execute(
                "UPDATE funds SET deelnemers_totaal = ? WHERE id = ? AND deelnemers_totaal IS NULL",
                (dtot, fid))
            new_deeln_totaal += r.rowcount
        if tax is not None:
            r = con.execute(
                "UPDATE funds SET eu_taxonomy_pct = ? WHERE id = ? AND eu_taxonomy_pct IS NULL",
                (tax, fid))
            new_tax += r.rowcount
    con.commit()
    log(f"apply: sfdr+={new_sfdr} actief+={new_deeln_actief} slapers+={new_deeln_slapers} "
        f"gepens+={new_deeln_gepens} totaal+={new_deeln_totaal} tax+={new_tax}")


if __name__ == "__main__":
    main()
