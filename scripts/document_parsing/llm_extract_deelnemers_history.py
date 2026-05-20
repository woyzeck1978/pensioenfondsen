"""LLM extractor for multi-year deelnemer tables (Meerjarenoverzicht).

Different from llm_extract.py — that one returns a single-year snapshot.
This one targets the kerncijfers table (typically 5-year wide) and
returns a per-year dict so we can fill historical_metrics rows back to
2020 or earlier.

Strategy:
  - For each PDF, score pages with a Kerncijfers/Meerjarenoverzicht
    header AND a deelnemers-numeric pattern (high confidence)
  - Take top 3 such pages
  - Single mistral-small call asking for {year: {actief, slapers,
    gepens, totaal}}
  - Validate + write to historical_metrics with NULL-only guard

Run from scripts/document_parsing/.
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

import fitz

OLLAMA = "http://100.71.107.24:11434"
MODEL = "mistral-small"
DB_PATH = "../../data/processed/pension_funds.db"
DIRS = ["../../data/annual_reports", "../../data/reports"]
LOG_DIR = "../../logs/llm_extract"

# Page selection: must have a strong "kerncijfers" header AND numeric deelnemer pattern
RE_KERNCIJFER_HEADER = re.compile(
    r"\b(?:kerncijfers|kerngegevens|meerjarenoverzicht|meerjaren-?overzicht)\b", re.I,
)
RE_DEELN_NUMERIC = re.compile(
    r"\b\d{2,3}(?:[.\s]\d{3})+\s+(?:actieve?|gewezen|gepens(?:ione)?|pensioengerecht|verzekerd|deelnemer)\b"
    r"|\b(?:actieve?|gewezen|gepens(?:ione)?|pensioengerecht|verzekerd|deelnemer)s?\s*[:\-]?\s*\d{2,3}(?:[.\s]\d{3})+",
    re.I,
)

PROMPT = """Je krijgt een fragment uit een Nederlands pensioenfonds-jaarverslag, \
specifiek het Kerncijfers / Meerjarenoverzicht. Daarin staat een tabel met deelnemer-aantallen \
over meerdere jaren (typisch 5 jaar).

Geef ALLEEN JSON terug, geen toelichting. Format:
{{
  "2024": {{"actief": int_or_null, "slapers": int_or_null, "gepensioneerd": int_or_null, "totaal": int_or_null}},
  "2023": {{...}},
  ...
}}

Regels:
- Voeg een jaar alleen toe als je expliciet aantallen voor dat jaar ziet.
- Integers zonder duizendpunt (10573, niet 10.573).
- Sla jaartallen-als-data over.
- "actief" = actieve / opbouwende / premiebetalende deelnemers
- "slapers" = gewezen deelnemers
- "gepensioneerd" = pensioengerechtigden / pensioenuitkeringen ontvangers
- "totaal" = totaal aantal deelnemers / verzekerden
- Geef voor velden die je niet ziet voor dat jaar: null

BRON:
{context}
"""


def build_inventory() -> dict[int, str]:
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


def find_meerjaren_pages(pdf_path: str, max_pages: int = 3) -> tuple[str, list[int]]:
    """Pages must have BOTH a kerncijfers-header AND a deelnemer-numeric hit."""
    doc = fitz.open(pdf_path)
    scored: list[tuple[int, int, str]] = []  # (score, page, text)
    for i, p in enumerate(doc):
        if i >= 200:
            break
        text = p.get_text()
        header = len(RE_KERNCIJFER_HEADER.findall(text))
        numeric = len(RE_DEELN_NUMERIC.findall(text))
        if header == 0 or numeric == 0:
            continue
        scored.append((header * 5 + numeric, i + 1, text))
    doc.close()
    if not scored:
        return "", []
    scored.sort(reverse=True)
    selected = scored[:max_pages]
    pages = sorted(s[1] for s in selected)
    ctx = "\n\n--- BREAK ---\n\n".join(
        f"[page {p}]\n{t.strip()[:2500]}" for s, p, t in sorted(selected, key=lambda x: x[1])
    )
    return ctx, pages


def call_ollama(prompt: str, timeout: int = 600) -> tuple[dict, float]:
    body = json.dumps({
        "model": MODEL, "prompt": prompt, "stream": False, "format": "json",
        "options": {"num_ctx": 12000, "temperature": 0.1, "num_predict": 600},
    }).encode()
    req = urllib.request.Request(
        f"{OLLAMA}/api/generate", data=body,
        headers={"Content-Type": "application/json"},
    )
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        resp = json.load(r)
    return resp, time.time() - t0


MIN_VAL, MAX_VAL = 50, 5_000_000


def sanitize_year_dict(d: dict) -> dict[int, dict]:
    """Validate each year's numbers."""
    today = date.today().year
    out: dict[int, dict] = {}
    for year_str, vals in (d or {}).items():
        try:
            year = int(year_str)
        except Exception:
            continue
        if not (2010 <= year <= today):
            continue
        clean: dict[str, int | None] = {}
        for k in ("actief", "slapers", "gepensioneerd", "totaal"):
            v = vals.get(k) if isinstance(vals, dict) else None
            if v is None:
                clean[k] = None
                continue
            try:
                v = int(v)
            except Exception:
                clean[k] = None
                continue
            if not (MIN_VAL <= v <= MAX_VAL):
                clean[k] = None; continue
            if 1990 <= v <= today + 5:
                clean[k] = None; continue  # year confused as count
            clean[k] = v
        if any(clean.values()):
            out[year] = clean
    return out


def ensure_schema(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS llm_deelnemers_history (
            fund_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            actief INTEGER, slapers INTEGER, gepensioneerd INTEGER, totaal INTEGER,
            pages_used TEXT, latency_s REAL,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (fund_id, year)
        )
    """)
    con.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--funds", type=str, default="")
    ap.add_argument("--top", type=int, default=0,
                    help="only top-N largest funds by AUM")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f"deelnemers_hist_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    log_f = open(log_path, "a", buffering=1)
    def log(m):
        line = f"[{datetime.now().strftime('%H:%M:%S')}] {m}"
        print(line, flush=True)
        log_f.write(line + "\n")

    con = sqlite3.connect(DB_PATH)
    ensure_schema(con)

    if args.apply:
        apply_to_history(con, log)
        con.close()
        return

    inv = build_inventory()
    if args.funds:
        targets = [int(x) for x in args.funds.split(",") if x.strip() and int(x) in inv]
    elif args.top:
        rows = con.execute("""
            SELECT id FROM funds WHERE aum_euro_bn IS NOT NULL
            ORDER BY aum_euro_bn DESC LIMIT ?
        """, (args.top,)).fetchall()
        targets = [r[0] for r in rows if r[0] in inv]
    else:
        targets = sorted(inv.keys())

    if args.limit:
        targets = targets[:args.limit]
    if args.resume:
        done = {r[0] for r in con.execute("SELECT DISTINCT fund_id FROM llm_deelnemers_history").fetchall()}
        targets = [t for t in targets if t not in done]

    n = len(targets)
    log(f"start | targets={n} | log={log_path}")
    t_start = time.time()
    n_hits = 0; n_skip = 0; n_err = 0; n_year_rows = 0

    for i, fid in enumerate(targets, 1):
        pdf = inv[fid]
        try:
            ctx, pages = find_meerjaren_pages(pdf)
            if not ctx:
                log(f"[{i}/{n}] fid={fid} {os.path.basename(pdf)[:50]} SKIP no kerncijfers page")
                n_skip += 1
                continue
            resp, dt = call_ollama(PROMPT.format(context=ctx))
            try:
                raw = json.loads(resp.get("response", ""))
            except Exception:
                log(f"[{i}/{n}] fid={fid} JSON parse failed after {dt:.0f}s")
                n_err += 1
                continue
            clean = sanitize_year_dict(raw)
            if not clean:
                log(f"[{i}/{n}] fid={fid} {dt:>4.0f}s 0 years found")
                continue
            n_hits += 1
            for year, v in clean.items():
                n_year_rows += 1
                con.execute("""
                    INSERT INTO llm_deelnemers_history
                      (fund_id, year, actief, slapers, gepensioneerd, totaal, pages_used, latency_s)
                    VALUES (?,?,?,?,?,?,?,?)
                    ON CONFLICT(fund_id, year) DO UPDATE SET
                      actief=excluded.actief, slapers=excluded.slapers,
                      gepensioneerd=excluded.gepensioneerd, totaal=excluded.totaal,
                      pages_used=excluded.pages_used, latency_s=excluded.latency_s,
                      extracted_at=CURRENT_TIMESTAMP
                """, (fid, year, v["actief"], v["slapers"], v["gepensioneerd"], v["totaal"],
                      json.dumps(pages), dt))
            con.commit()
            years_str = ", ".join(f"{y}=A{v.get('actief')}/T{v.get('totaal')}" for y, v in sorted(clean.items(), reverse=True)[:3])
            log(f"[{i}/{n}] fid={fid:>3} {dt:>4.0f}s {len(clean)}yrs  {years_str}")
        except urllib.error.URLError as e:
            log(f"[{i}/{n}] fid={fid} URL-ERR: {e}")
            n_err += 1
        except Exception as e:
            log(f"[{i}/{n}] fid={fid} ERR: {e}")
            n_err += 1

    log(f"done | {time.time()-t_start:.0f}s | hits={n_hits} year-rows={n_year_rows} skip={n_skip} err={n_err}")
    log_f.close()
    con.close()


def apply_to_history(con, log):
    """Copy llm_deelnemers_history -> historical_metrics, NULL-only."""
    rows = con.execute("""
        SELECT fund_id, year, actief, slapers, gepensioneerd, totaal
        FROM llm_deelnemers_history
    """).fetchall()
    new_act = new_sla = new_gep = new_tot = 0
    for fid, year, act, sla, gep, tot in rows:
        # Make sure a row exists; if not, create skeleton
        exists = con.execute(
            "SELECT 1 FROM historical_metrics WHERE fund_id=? AND year=?", (fid, year)
        ).fetchone()
        if not exists:
            con.execute("INSERT INTO historical_metrics (fund_id, year) VALUES (?, ?)", (fid, year))
        if act is not None:
            c = con.execute("UPDATE historical_metrics SET deelnemers_actief=? WHERE fund_id=? AND year=? AND deelnemers_actief IS NULL", (act, fid, year))
            new_act += c.rowcount
        if sla is not None:
            c = con.execute("UPDATE historical_metrics SET deelnemers_slapers=? WHERE fund_id=? AND year=? AND deelnemers_slapers IS NULL", (sla, fid, year))
            new_sla += c.rowcount
        if gep is not None:
            c = con.execute("UPDATE historical_metrics SET deelnemers_pensioengerechtigd=? WHERE fund_id=? AND year=? AND deelnemers_pensioengerechtigd IS NULL", (gep, fid, year))
            new_gep += c.rowcount
        if tot is not None:
            c = con.execute("UPDATE historical_metrics SET deelnemers_totaal=? WHERE fund_id=? AND year=? AND deelnemers_totaal IS NULL", (tot, fid, year))
            new_tot += c.rowcount
    con.commit()
    log(f"apply: actief+={new_act} slapers+={new_sla} gepens+={new_gep} totaal+={new_tot}")


if __name__ == "__main__":
    main()
