"""LLM-gegenereerde analyse van een jaarverslag-PDF.

Voor elke fund × fiscal_year wordt een gestructureerde JSON gevraagd
aan de lokale Ollama (mistral-small):

  {"summary":           "<3-5 zin samenvatting>",
   "highlights":        ["...", "...", "..."],   # 3-5 positieve punten
   "lowlights":         ["...", "...", "..."],   # 3-5 aandachtspunten
   "key_risks":         ["...", "...", "..."],
   "transitie_status":  "<1 zin over WTP-status>"}

Bron-pagina's worden gefilterd op headers die meestal de relevante
narrative bevatten: 'Voorwoord', 'Bestuursverslag', 'Samenvatting',
'Belangrijkste ontwikkelingen', 'Strategie en beleid', 'Risico'.

Output naar tabel fund_analysis (PRIMARY KEY fund_id + fiscal_year).
Re-run zonder --force is een no-op voor reeds verwerkte rijen.

Usage:
  python3 llm_extract_analysis.py --funds 13,111,32     # 3 FY2025 PDFs
  python3 llm_extract_analysis.py --top 30
  python3 llm_extract_analysis.py --funds 111 --force   # overschrijf
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
from datetime import datetime

import fitz

OLLAMA = "http://100.71.107.24:11434"
MODEL = "mistral-small"
DB_PATH = "../../data/processed/pension_funds.db"
DIRS = ["../../data/annual_reports", "../../data/reports"]
LOG_DIR = "../../logs/llm_extract"

# Page selection: prefer narrative pages (voorwoord/bestuursverslag) over
# pure-numbers pages (kerncijfers). Score = header matches + sentence count.
HEADER_PATTERNS = [
    re.compile(r"\bvoorwoord\b", re.I),
    re.compile(r"\bbestuursverslag\b", re.I),
    re.compile(r"\bsamenvatting\b", re.I),
    re.compile(r"\bbelangrijkste\s+(?:ontwikkelingen|gebeurtenissen|punten)\b", re.I),
    re.compile(r"\bstrategie\s+en\s+beleid\b", re.I),
    re.compile(r"\brisicobeheersing\b|\brisicoparagraaf\b", re.I),
    re.compile(r"\bbericht\s+van\s+(?:het\s+)?bestuur\b", re.I),
    re.compile(r"\bvooruitblik\b|\bvooruitzicht\b", re.I),
]


def build_inventory() -> dict[int, list[tuple[int, str]]]:
    by_fund = defaultdict(list)
    for d in DIRS:
        for path in glob.glob(f"{d}/*.pdf"):
            base = os.path.basename(path)
            m = re.match(r"^(\d+)[_\s]", base)
            if not m:
                continue
            ym = re.search(r"(20\d{2})", base)
            by_fund[int(m.group(1))].append((int(ym.group(1)) if ym else 0, path))
    return by_fund


def pick_pdf_for_year(items, prefer_year: int | None):
    """Return (year, path). If prefer_year given and present, use that. Else newest."""
    if prefer_year is not None:
        for y, p in items:
            if y == prefer_year:
                return y, p
    return sorted(items, reverse=True)[0]


def find_narrative_pages(pdf_path: str, max_pages: int = 5) -> tuple[str, list[int], int]:
    doc = fitz.open(pdf_path)
    scored = []  # (score, page_no, text)
    for i, page in enumerate(doc):
        if i >= 200:
            break
        text = page.get_text()
        header_score = sum(len(p.findall(text)) for p in HEADER_PATTERNS)
        # Sentence-ish density (skip tables/numeric-only)
        sentence_score = min(5, len(re.findall(r"\.\s+[A-Z]", text)))
        if header_score == 0 and sentence_score < 2:
            continue
        score = header_score * 8 + sentence_score
        scored.append((score, i + 1, text))
    n_pages = len(doc)
    doc.close()
    if not scored:
        return "", [], n_pages
    scored.sort(reverse=True)
    chosen = sorted(scored[:max_pages], key=lambda x: x[1])  # original order
    pages = [c[1] for c in chosen]
    ctx = "\n\n--- BREAK ---\n\n".join(
        f"[page {n}]\n{t.strip()[:2400]}" for _, n, t in chosen
    )
    return ctx, pages, n_pages


PROMPT = """Je krijgt fragmenten uit een Nederlands pensioenfonds-jaarverslag \
(voorwoord, bestuursverslag, samenvatting). Geef ALLEEN JSON terug \
— geen toelichting, geen markdown.

Schema (alle velden in het Nederlands):
{{
  "summary":           "<3-5 zinnen, max 80 woorden, hoofdpunten van het jaar>",
  "highlights":        ["<bullet 1>", "<bullet 2>", "<bullet 3>", "<eventueel 4>", "<eventueel 5>"],
  "lowlights":         ["<bullet 1>", "<bullet 2>", "<eventueel 3>"],
  "key_risks":         ["<bullet 1>", "<bullet 2>", "<eventueel 3>"],
  "transitie_status":  "<1-2 zinnen over de Wtp-transitie / invaren-status>"
}}

Regels:
- Schrijf neutraal, feitelijk. Geen marketing-taal.
- Maximaal 25 woorden per bullet.
- highlights = positieve / behaalde resultaten.
- lowlights = teleurstellende ontwikkelingen of zorgpunten.
- key_risks = financiële, operationele of regelgevingsrisico's die het verslag noemt.
- transitie_status: vermeld of het fonds is ingevaren, een datum heeft, of nog beslist.
- Als een sectie geen materiaal heeft, geef een lege array [].
- GEEN andere velden dan in schema.

BRON:
{context}
"""


def call_ollama(prompt: str, timeout: int = 600) -> tuple[dict, float]:
    body = json.dumps({
        "model": MODEL, "prompt": prompt, "stream": False, "format": "json",
        "options": {"num_ctx": 16000, "temperature": 0.2, "num_predict": 700},
    }).encode()
    req = urllib.request.Request(
        f"{OLLAMA}/api/generate", data=body,
        headers={"Content-Type": "application/json"},
    )
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        resp = json.load(r)
    return resp, time.time() - t0


def sanitize(d: dict) -> dict:
    """Normalize types & guard against junk."""
    out = {"summary": None, "highlights": [], "lowlights": [], "key_risks": [], "transitie_status": None}
    if not isinstance(d, dict):
        return out
    s = d.get("summary")
    if isinstance(s, str) and s.strip():
        out["summary"] = s.strip()[:800]
    for k in ("highlights", "lowlights", "key_risks"):
        v = d.get(k)
        if isinstance(v, list):
            cleaned = [str(x).strip()[:300] for x in v if isinstance(x, (str, int, float)) and str(x).strip()]
            out[k] = cleaned[:5]
    t = d.get("transitie_status")
    if isinstance(t, str) and t.strip():
        out["transitie_status"] = t.strip()[:400]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--funds", type=str, default="", help="comma-separated fund ids")
    ap.add_argument("--top", type=int, default=0, help="top-N by AUM")
    ap.add_argument("--fiscal-year", type=int, default=None,
                    help="prefer this fiscal year (default: newest PDF)")
    ap.add_argument("--force", action="store_true",
                    help="overwrite existing fund_analysis rows")
    args = ap.parse_args()

    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f"analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    log_f = open(log_path, "a", buffering=1)
    def log(m):
        line = f"[{datetime.now().strftime('%H:%M:%S')}] {m}"
        print(line, flush=True)
        log_f.write(line + "\n")

    inv = build_inventory()
    con = sqlite3.connect(DB_PATH)

    if args.funds:
        targets = [int(x) for x in args.funds.split(",") if x.strip()]
    elif args.top:
        rows = con.execute("""
            SELECT id FROM funds WHERE aum_euro_bn IS NOT NULL
            ORDER BY aum_euro_bn DESC LIMIT ?
        """, (args.top,)).fetchall()
        targets = [r[0] for r in rows]
    else:
        targets = sorted(inv.keys())

    targets = [t for t in targets if t in inv]
    if not args.force:
        done = {(r[0], r[1]) for r in con.execute(
            "SELECT fund_id, fiscal_year FROM fund_analysis"
        ).fetchall()}
    else:
        done = set()

    n = len(targets)
    log(f"start | {n} fund(s) | year={args.fiscal_year or 'newest'} | force={args.force}")
    t0 = time.time()
    hits = skips = errs = 0

    for i, fid in enumerate(targets, 1):
        try:
            year, pdf = pick_pdf_for_year(inv[fid], args.fiscal_year)
            if not args.force and (fid, year) in done:
                log(f"[{i}/{n}] fid={fid} {year} already done — skip")
                skips += 1
                continue
            ctx, pages, n_pdf = find_narrative_pages(pdf)
            if not ctx:
                log(f"[{i}/{n}] fid={fid} {year} {os.path.basename(pdf)[:50]} SKIP no narrative pages")
                skips += 1
                continue
            resp, dt = call_ollama(PROMPT.format(context=ctx))
            try:
                raw = json.loads(resp.get("response", ""))
            except Exception as e:
                log(f"[{i}/{n}] fid={fid} JSON parse failed: {e!r}")
                errs += 1
                continue
            clean = sanitize(raw)
            con.execute("""
                INSERT INTO fund_analysis
                    (fund_id, fiscal_year, summary, highlights_json,
                     lowlights_json, key_risks_json, transitie_status,
                     source_pdf, generated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(fund_id, fiscal_year) DO UPDATE SET
                    summary=excluded.summary,
                    highlights_json=excluded.highlights_json,
                    lowlights_json=excluded.lowlights_json,
                    key_risks_json=excluded.key_risks_json,
                    transitie_status=excluded.transitie_status,
                    source_pdf=excluded.source_pdf,
                    generated_at=CURRENT_TIMESTAMP
            """, (fid, year, clean["summary"],
                  json.dumps(clean["highlights"], ensure_ascii=False),
                  json.dumps(clean["lowlights"], ensure_ascii=False),
                  json.dumps(clean["key_risks"], ensure_ascii=False),
                  clean["transitie_status"], os.path.relpath(pdf, "../..")))
            con.commit()
            hits += 1
            log(f"[{i}/{n}] fid={fid} FY{year} {dt:>4.0f}s  "
                f"sum={'Y' if clean['summary'] else 'N'} "
                f"hl={len(clean['highlights'])} ll={len(clean['lowlights'])} "
                f"risks={len(clean['key_risks'])}")
        except urllib.error.URLError as e:
            log(f"[{i}/{n}] fid={fid} URL-ERR: {e}")
            errs += 1
        except Exception as e:
            log(f"[{i}/{n}] fid={fid} ERR: {e}")
            errs += 1

    log(f"done | {time.time()-t0:.0f}s | hits={hits} skips={skips} errors={errs}")
    log_f.close()
    con.close()


if __name__ == "__main__":
    main()
