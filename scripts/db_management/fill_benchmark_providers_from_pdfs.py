"""Fill funds.benchmark_providers for funds that are still NULL, by scanning
all locally available annual-report PDFs for index-provider tokens.

Differs from analyze_benchmarks.py (which extracted full index *names* from
data/reports only): this scans data/annual_reports + data/reports +
data/historical_reports and only detects the *provider*, which needs far less
context and therefore has much higher recall.

False-positive control: unambiguous provider tokens (MSCI, FTSE, GRESB, ...)
count anywhere in the document; ambiguous tokens (AEX, Euribor, Nasdaq,
S&P, Citi) only count when they appear near benchmark vocabulary.

Deterministic — no LLM, no network. NULL-only by design: existing
benchmark_providers values are never touched.

Usage:
    python3 fill_benchmark_providers_from_pdfs.py            # dry-run
    python3 fill_benchmark_providers_from_pdfs.py --apply

Rollback: the script prints the touched fund ids; set those rows back to
NULL, or restore data/processed/pension_funds.db from git.
"""
import argparse
import os
import re
import sqlite3

import fitz  # PyMuPDF

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

# Volgorde = voorkeur: recentste jaarverslagen eerst.
PDF_DIRS = ["data/annual_reports", "data/reports", "data/historical_reports"]

# Overal-geldige tokens: in een pensioenjaarverslag vrijwel altijd benchmarkgebruik.
STRONG = [
    (r"msci", "MSCI"),
    (r"\bftse\b|\brussell\b", "FTSE Russell"),
    (r"bloomberg|barclays", "Bloomberg (Barclays)"),
    (r"j\.?p\.?\s?morgan\s?(?:gbi|embi|index|benchmark)|gbi[- ]em|embi", "J.P. Morgan"),
    (r"\bbofa\b|merrill lynch", "ICE BofA"),
    (r"iboxx", "S&P Dow Jones (incl. iBoxx)"),
    (r"\bstoxx\b", "STOXX"),
    (r"\bgresb\b", "GRESB (vastgoed-ESG)"),
    (r"solactive", "Solactive"),
    (r"refinitiv", "Refinitiv (LSEG)"),
]
# Alleen geldig dicht bij benchmark-vocabulaire (anders te veel ruis):
CONTEXT = [
    (r"s\s?&\s?p|dow jones", "S&P Dow Jones (incl. iBoxx)"),
    (r"\baex\b", "Euronext (AEX)"),
    (r"euribor|eonia|\bsonia\b|\b€str\b|\bestr\b", "Rente-fixing (Euribor e.d.)"),
    (r"nasdaq", "Nasdaq"),
    (r"\bciti\b(?!group|zen|bank)", "FTSE Russell"),
]
BENCH_VOCAB = r"benchmark|graadmeter|normportefeuille|referentie[- ]?index|index|meetlat"
WINDOW = 300  # tekens rond het token waarin benchmark-vocabulaire moet staan

YEAR_RE = re.compile(r"(20\d{2})")


def pdf_text(path, max_pages=150):
    try:
        doc = fitz.open(path)
        txt = " ".join(doc.load_page(i).get_text()
                       for i in range(min(max_pages, len(doc))))
        doc.close()
        return txt.lower()
    except Exception:
        return ""


def best_pdfs(fund_id):
    """Alle PDF's voor dit fonds, beste eerst (map-voorkeur, dan jaartal aflopend)."""
    out = []
    for rank, d in enumerate(PDF_DIRS):
        full = os.path.join(BASE_DIR, d)
        if not os.path.isdir(full):
            continue
        for f in os.listdir(full):
            m = re.match(rf"^{fund_id}_.*\.pdf$", f)
            if m:
                years = YEAR_RE.findall(f)
                year = int(max(years)) if years else 0
                out.append((rank, -year, os.path.join(full, f)))
    return [p for _, _, p in sorted(out)]


def providers_in(text):
    found = set()
    for pat, canon in STRONG:
        if re.search(pat, text):
            found.add(canon)
    for pat, canon in CONTEXT:
        for m in re.finditer(pat, text):
            lo = max(0, m.start() - WINDOW)
            if re.search(BENCH_VOCAB, text[lo:m.end() + WINDOW]):
                found.add(canon)
                break
    return sorted(found)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="schrijf naar de DB (default: dry-run)")
    ap.add_argument("--max-pdfs", type=int, default=2,
                    help="max aantal PDF's per fonds proberen (default 2)")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    targets = con.execute(
        "SELECT id, name FROM funds WHERE benchmark_providers IS NULL ORDER BY name"
    ).fetchall()

    plan, no_pdf, no_hit = [], [], []
    for fid, name in targets:
        pdfs = best_pdfs(fid)
        if not pdfs:
            no_pdf.append((fid, name))
            continue
        provs, used = [], None
        for path in pdfs[:args.max_pdfs]:
            text = pdf_text(path)
            provs = providers_in(text)
            if provs:
                used = os.path.relpath(path, BASE_DIR)
                break
        if provs:
            plan.append((fid, name, ", ".join(provs), used))
        else:
            no_hit.append((fid, name, os.path.relpath(pdfs[0], BASE_DIR)))

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"=== {mode} — benchmark_providers aanvullen vanuit lokale PDF's ===")
    print(f"Fondsen zonder waarde: {len(targets)} | met PDF-treffer: {len(plan)} | "
          f"PDF zonder treffer: {len(no_hit)} | geen lokale PDF: {len(no_pdf)}\n")

    for fid, name, val, src in plan:
        print(f"  id={fid:<4} {name[:42]:44s} -> {val}")
        print(f"          bron: {src}")
    if no_hit:
        print("\nPDF aanwezig, geen provider gevonden (blijft NULL):")
        for fid, name, src in no_hit:
            print(f"  id={fid:<4} {name[:40]:42s} | {src}")
    if no_pdf:
        print("\nGeen lokale PDF (fase 2 — download via scraped_documents):")
        for fid, name in no_pdf:
            print(f"  id={fid:<4} {name[:60]}")

    if not args.apply:
        print("\nDry-run: er is NIETS geschreven. Draai met --apply om uit te voeren.")
        con.close()
        return

    cur = con.cursor()
    for fid, _, val, _ in plan:
        # NULL-only: alleen rijen die nu NULL zijn (target-selectie garandeert dat al)
        cur.execute(
            "UPDATE funds SET benchmark_providers = ? "
            "WHERE id = ? AND benchmark_providers IS NULL", (val, fid))
    con.commit()
    con.close()
    print(f"\nKlaar: {len(plan)} rijen geschreven (NULL-only).")


if __name__ == "__main__":
    main()
