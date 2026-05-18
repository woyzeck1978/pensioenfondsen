"""Extract deelnemers + SFDR-classificatie uit lokale jaarverslag PDFs.

Strategie:
  - Per fund_id: kies de meest recente PDF in data/annual_reports of
    data/reports (bestandsnaam-jaar tellen).
  - SFDR: zoek naar specifieke classificatie-formuleringen rond 'artikel
    6/8/9' EN 'SFDR'. Klassieke false positives ('artikel 4 SFDR',
    'artikel 8 van de Wet op…') worden vermeden door context-eisen.
  - Deelnemers: inline regex 'N (actieve|gewezen|gepensioneerde)
    deelnemers'. Max-value sanity: actief/slapers/gepens elk < 10M;
    cijfer mag niet meer dan 7 digits hebben (rejects tabel-rows die
    door pymupdf zijn gemerged tot één lange getal-string).

Output: scripts/document_parsing/extract_pdf_metadata.py inserts/updates
rows in fy_annual_metrics with notes='extracted from <filename>'.

Run from scripts/document_parsing/:
    python3 extract_pdf_metadata.py             # all funds with PDFs
    python3 extract_pdf_metadata.py --limit 5
    python3 extract_pdf_metadata.py --apply     # also update funds.* NULLs
"""
import argparse
import glob
import os
import re
import sqlite3
import time
from collections import defaultdict

import fitz  # pymupdf

DB_PATH = "../../data/processed/pension_funds.db"
DIRS = ["../../data/annual_reports", "../../data/reports"]
MAX_PAGES = 250
MAX_DEELN = 10_000_000  # 10M cap rejects tabular merges (Beroepsvervoer's "200228196941…")

# ── SFDR ───────────────────────────────────────────────────────────────
# Article 6/8/9 only. Phrases that strongly imply this fund's classification.
SFDR_PATTERNS = [
    # "classificatie ... artikel 8" / "wordt geclassificeerd als ... artikel 8"
    re.compile(r"(?:classific|classified|classification|valt\s+onder|"
               r"geldt\s+als|aangemerkt\s+als|kwalificeert\s+als|"
               r"fonds\s+(?:onder|valt\s+onder)|onder\s+artikel)"
               r"[\s\S]{0,60}?(?:artikel|article)\s*(6|8|9)\b", re.I),
    # "fonds onder artikel 8 van de SFDR valt"
    re.compile(r"(?:fonds|product)\s+onder\s+(?:artikel|article)\s*(6|8|9)\s*(?:van\s+de\s+)?sfdr", re.I),
    # "artikel 8 SFDR-product" / "artikel 8, SFDR" / "artikel 8 SFDR"
    re.compile(r"(?:artikel|article)\s*(6|8|9)\s+sfdr(?:[\s\-]product|[\s\-]fonds)?\b", re.I),
    # "Article 8 of SFDR"
    re.compile(r"article\s+(6|8|9)\s+(?:of\s+(?:the\s+)?)?sfdr", re.I),
]


def detect_sfdr(text: str) -> int | None:
    """Return 6, 8, 9, or None — pick highest priority (9 > 8 > 6)."""
    seen: set[int] = set()
    for pat in SFDR_PATTERNS:
        for m in pat.finditer(text):
            seen.add(int(m.group(1)))
            if len(seen) >= 3:
                break
    if 9 in seen: return 9
    if 8 in seen: return 8
    if 6 in seen: return 6
    return None


# ── Deelnemers ─────────────────────────────────────────────────────────
NUM_RAW = r"(\d{1,3}(?:[.\s]\d{3}){1,2}|\d{3,7})"  # 1-2 thousand groups OR 3-7 plain digits
RE_ACTIEF = re.compile(rf"\b{NUM_RAW}\s+actieve\s+deelnemers\b", re.I)
RE_SLAPERS = re.compile(rf"\b{NUM_RAW}\s+(?:gewezen\s+deelnemers|slapers)\b", re.I)
RE_GEPENS = re.compile(rf"\b{NUM_RAW}\s+(?:gepensioneerden|pensioengerechtigden|pensioenuitkering(?:en|sgerechtigden))\b", re.I)
RE_TOTAAL = re.compile(rf"(?:totaal\s+aantal\s+deelnemers|aantal\s+deelnemers\s*[:\-])\s*{NUM_RAW}\b", re.I)


def to_int_safe(s: str) -> int | None:
    cleaned = re.sub(r"[\s.]", "", s)
    if not cleaned.isdigit():
        return None
    try:
        v = int(cleaned)
    except Exception:
        return None
    if v < 100 or v > MAX_DEELN:
        return None
    return v


def first_match(pat, text):
    m = pat.search(text)
    if not m:
        return None
    return to_int_safe(m.group(1))


# ── Inventory ──────────────────────────────────────────────────────────
def build_inventory():
    by_fund: dict[int, list[tuple[int, str]]] = defaultdict(list)
    for d in DIRS:
        for path in glob.glob(f"{d}/*.pdf"):
            base = os.path.basename(path)
            m = re.match(r"^(\d+)[_\s]", base)
            if not m:
                continue
            fid = int(m.group(1))
            ym = re.search(r"(20\d{2})", base)
            year = int(ym.group(1)) if ym else 0
            by_fund[fid].append((year, path))
    # Newest first
    return {fid: sorted(items, reverse=True)[0] for fid, items in by_fund.items()}


def extract_one(fid: int, year: int, path: str) -> dict:
    out = {"fund_id": fid, "pdf_year": year or None, "pdf_path": path,
           "actief": None, "slapers": None, "gepens": None, "totaal": None,
           "sfdr_article": None}
    try:
        doc = fitz.open(path)
    except Exception:
        return out
    try:
        full_parts: list[str] = []
        for i, page in enumerate(doc):
            if i >= MAX_PAGES:
                break
            try:
                full_parts.append(page.get_text())
            except Exception:
                pass
        full = "\n".join(full_parts)
    finally:
        doc.close()

    out["actief"] = first_match(RE_ACTIEF, full)
    out["slapers"] = first_match(RE_SLAPERS, full)
    out["gepens"] = first_match(RE_GEPENS, full)
    out["totaal"] = first_match(RE_TOTAAL, full)
    out["sfdr_article"] = detect_sfdr(full)

    # If we have components but no totaal, derive
    if out["totaal"] is None:
        parts = [v for v in (out["actief"], out["slapers"], out["gepens"]) if v]
        if len(parts) >= 2:
            out["totaal"] = sum(parts)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--apply", action="store_true",
                    help="Also UPDATE funds.* NULLs from the extracted values")
    args = ap.parse_args()

    inventory = build_inventory()
    print(f"PDFs by fund: {len(inventory)}")

    con = sqlite3.connect(DB_PATH)
    db_ids = {r[0] for r in con.execute("SELECT id FROM funds").fetchall()}
    items = [(fid, *v) for fid, v in inventory.items() if fid in db_ids]
    if args.limit:
        items = items[:args.limit]
    n = len(items)
    print(f"Will process {n} PDFs")

    results: list[dict] = []
    start = time.time()
    for i, (fid, year, path) in enumerate(items, 1):
        r = extract_one(fid, year, path)
        results.append(r)
        if i % 25 == 0 or i == n:
            print(f"  [{i}/{n}] rate={i/max(time.time()-start, 0.1):.1f}/s")

    hits_sfdr = sum(1 for r in results if r["sfdr_article"])
    hits_actief = sum(1 for r in results if r["actief"])
    hits_totaal = sum(1 for r in results if r["totaal"])
    print(f"\nHits: sfdr={hits_sfdr}, actief={hits_actief}, totaal={hits_totaal}")

    if not args.apply:
        # Just write findings to a scratch table for review
        con.execute("""
            CREATE TABLE IF NOT EXISTS pdf_extracted (
                fund_id INTEGER PRIMARY KEY,
                pdf_year INTEGER, pdf_path TEXT,
                actief INTEGER, slapers INTEGER, gepens INTEGER, totaal INTEGER,
                sfdr_article INTEGER,
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(fund_id) REFERENCES funds(id)
            )
        """)
        for r in results:
            con.execute("""
                INSERT INTO pdf_extracted (fund_id, pdf_year, pdf_path, actief, slapers, gepens, totaal, sfdr_article)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(fund_id) DO UPDATE SET
                    pdf_year=excluded.pdf_year, pdf_path=excluded.pdf_path,
                    actief=excluded.actief, slapers=excluded.slapers,
                    gepens=excluded.gepens, totaal=excluded.totaal,
                    sfdr_article=excluded.sfdr_article,
                    extracted_at=CURRENT_TIMESTAMP
            """, (r["fund_id"], r["pdf_year"], r["pdf_path"],
                  r["actief"], r["slapers"], r["gepens"], r["totaal"], r["sfdr_article"]))
        con.commit()
        print(f"Wrote {len(results)} rows to pdf_extracted (no funds.* updates — use --apply)")
    else:
        # Apply: only fill NULLs in funds
        new_actief = new_slapers = new_gepens = new_totaal = new_sfdr = 0
        for r in results:
            fid = r["fund_id"]
            if r["actief"]:
                c = con.execute("UPDATE funds SET deelnemers_actief = ? WHERE id = ? AND deelnemers_actief IS NULL", (r["actief"], fid))
                new_actief += c.rowcount
            if r["slapers"]:
                c = con.execute("UPDATE funds SET deelnemers_slapers = ? WHERE id = ? AND deelnemers_slapers IS NULL", (r["slapers"], fid))
                new_slapers += c.rowcount
            if r["gepens"]:
                c = con.execute("UPDATE funds SET deelnemers_gepensioneerd = ? WHERE id = ? AND deelnemers_gepensioneerd IS NULL", (r["gepens"], fid))
                new_gepens += c.rowcount
            if r["totaal"]:
                c = con.execute("UPDATE funds SET deelnemers_totaal = ? WHERE id = ? AND deelnemers_totaal IS NULL", (r["totaal"], fid))
                new_totaal += c.rowcount
            if r["sfdr_article"]:
                c = con.execute("UPDATE funds SET sfdr_article = ? WHERE id = ? AND sfdr_article IS NULL", (r["sfdr_article"], fid))
                new_sfdr += c.rowcount
        con.commit()
        print(f"\n--apply: filled NULLs in funds:")
        print(f"  deelnemers_actief:        +{new_actief}")
        print(f"  deelnemers_slapers:       +{new_slapers}")
        print(f"  deelnemers_gepensioneerd: +{new_gepens}")
        print(f"  deelnemers_totaal:        +{new_totaal}")
        print(f"  sfdr_article:             +{new_sfdr}")

    con.close()


if __name__ == "__main__":
    main()
