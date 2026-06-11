"""Derive canonical benchmark providers from funds.benchmarks.

funds.benchmarks holds raw, regex-extracted index names from annual-report
PDFs (extract_benchmarks.py). This script maps those strings onto a fixed
set of canonical provider names and writes them to funds.benchmark_providers
(comma-separated, alphabetical).

Deterministic — no LLM, no network. Re-runnable: derived values only.

Usage:
    python3 derive_benchmark_providers.py            # dry-run (default)
    python3 derive_benchmark_providers.py --apply    # write NULL fields only
    python3 derive_benchmark_providers.py --apply --force   # overwrite existing

Rollback:
    UPDATE funds SET benchmark_providers = NULL;
    (column is fully derived; re-run this script to regenerate)
"""
import argparse
import os
import re
import sqlite3

# Self-locating DB path (works from any cwd) — same pattern as dashboard.py
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

# token-regex -> canonical provider. Notes:
# - Citi fixed-income indices were acquired by FTSE -> FTSE Russell.
# - iBoxx (IHS Markit) is S&P since 2022.
# - GRESB is a real-estate ESG benchmark, not a market index provider —
#   kept, but labelled as such.
# - Euribor/Eonia/Sonia are rate fixings, not index providers — labelled.
PROVIDERS = [
    (r"msci", "MSCI"),
    (r"ftse|russell|citi", "FTSE Russell"),
    (r"bloomberg|barclays", "Bloomberg (Barclays)"),
    (r"j\.?p\.?\s?morgan|gbi[- ]em", "J.P. Morgan"),
    (r"bofa|merrill", "ICE BofA"),
    (r"s&p|dow jones|iboxx", "S&P Dow Jones (incl. iBoxx)"),
    (r"stoxx", "STOXX"),
    (r"nasdaq", "Nasdaq"),
    (r"gresb", "GRESB (vastgoed-ESG)"),
    (r"solactive", "Solactive"),
    (r"refinitiv", "Refinitiv (LSEG)"),
    (r"\baex\b", "Euronext (AEX)"),
    (r"euribor|eonia|sonia|\bestr\b", "Rente-fixing (Euribor e.d.)"),
]

# AEX-treffers zijn vaak ruis (jaarverslagen noemen de AEX buiten
# benchmark-context); in de dry-run apart gemarkeerd voor handmatige review.
SUSPECT = {"Euronext (AEX)"}


def providers_for(text: str) -> list:
    t = (text or "").lower()
    return sorted({canon for pat, canon in PROVIDERS if re.search(pat, t)})


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="schrijf naar de DB (default: dry-run)")
    ap.add_argument("--force", action="store_true",
                    help="overschrijf ook bestaande benchmark_providers-waarden")
    ap.add_argument("--skip-suspect", action="store_true",
                    help="laat verdachte treffers (AEX) weg uit de waarde")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    have_col = any(r[1] == "benchmark_providers"
                   for r in cur.execute("PRAGMA table_info(funds)"))

    rows = cur.execute(
        "SELECT id, name, benchmarks, "
        + ("benchmark_providers" if have_col else "NULL")
        + " FROM funds WHERE benchmarks IS NOT NULL AND TRIM(benchmarks) <> '' "
        "AND benchmarks NOT LIKE '%None explicitly%' ORDER BY name"
    ).fetchall()

    plan = []     # (id, name, new_value, old_value, suspect_hits)
    skipped = []  # geen provider herkend
    for fid, name, bench, existing in rows:
        provs = providers_for(bench)
        if args.skip_suspect:
            provs = [p for p in provs if p not in SUSPECT]
        if not provs:
            skipped.append((fid, name, bench))
            continue
        plan.append((fid, name, ", ".join(provs), existing,
                     sorted(set(provs) & SUSPECT)))

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"=== {mode} — benchmark_providers afleiden uit funds.benchmarks ===")
    print(f"DB: {DB_PATH}")
    print(f"Kolom bestaat al: {'ja' if have_col else 'nee (wordt aangemaakt bij --apply)'}")
    print(f"Kandidaten: {len(plan)} schrijfacties, {len(skipped)} zonder herkende provider\n")

    for fid, name, val, existing, sus in plan:
        marker = "  ⚠ AEX-treffer, handmatig verifiëren" if sus else ""
        guard = ""
        if existing and not args.force:
            guard = "  [SKIP: bestaande waarde, gebruik --force]"
        print(f"  id={fid:<4} {name[:44]:46s} -> {val}{marker}{guard}")

    if skipped:
        print("\nGeen provider herkend (blijft NULL):")
        for fid, name, bench in skipped:
            print(f"  id={fid:<4} {name[:40]:42s} | {bench[:70]}")

    if not args.apply:
        print("\nDry-run: er is NIETS geschreven. Draai met --apply om uit te voeren.")
        con.close()
        return

    if not have_col:
        cur.execute("ALTER TABLE funds ADD COLUMN benchmark_providers TEXT")
    written = 0
    for fid, name, val, existing, _ in plan:
        if existing and not args.force:
            continue
        cur.execute("UPDATE funds SET benchmark_providers = ? WHERE id = ?", (val, fid))
        written += 1
    con.commit()
    con.close()
    print(f"\nKlaar: {written} rijen geschreven"
          + ("" if args.force else " (NULL-only-guard actief)") + ".")


if __name__ == "__main__":
    main()
