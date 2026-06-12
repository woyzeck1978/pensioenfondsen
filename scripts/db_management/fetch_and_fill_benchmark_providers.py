"""Fase 2: benchmark_providers aanvullen via PDF's uit scraped_documents.

Voor fondsen waar benchmark_providers nog NULL is (na de lokale-PDF-pass van
fill_benchmark_providers_from_pdfs.py) downloadt dit script jaarverslag-achtige
PDF's van de fondswebsites (URL's uit scraped_documents), scant ze op
provider-tokens en schrijft het resultaat NULL-only naar de DB.

Documentselectie is bewust strikt — alleen jaarverslag / jaarbericht /
annual report / ABTN / beleggingsbeginselen / beleggingsplan — zodat een
losse duurzaamheidsbrochure die "MSCI ESG ratings" noemt niet als
benchmark-provider meetelt.

Downloads landen in data/interim/benchmark_pdfs/ (.gitignored), max 2 per
fonds, 1.5s pauze tussen requests.

Usage:
    python3 fetch_and_fill_benchmark_providers.py              # dry-run
                                                               # (downloadt wel)
    python3 fetch_and_fill_benchmark_providers.py --apply
    python3 fetch_and_fill_benchmark_providers.py --no-download  # alleen cache

Rollback: geprinte fund-ids op NULL zetten, of DB uit git herstellen.
"""
import argparse
import os
import re
import sqlite3
import time

import fitz  # PyMuPDF
import requests

from fill_benchmark_providers_from_pdfs import providers_in, pdf_text  # zelfde logica

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")
CACHE_DIR = os.path.join(BASE_DIR, "data", "interim", "benchmark_pdfs")

DOC_FILTER = """
  doc_type = 'document' AND (
       lower(COALESCE(title,'')) LIKE '%jaarverslag%'
    OR lower(COALESCE(title,'')) LIKE '%jaarbericht%'
    OR lower(COALESCE(title,'')) LIKE '%annual report%'
    OR lower(COALESCE(title,'')) LIKE '%abtn%'
    OR lower(COALESCE(title,'')) LIKE '%beleggingsbeginselen%'
    OR lower(COALESCE(title,'')) LIKE '%beleggingsplan%'
    OR lower(COALESCE(url,''))   LIKE '%jaarverslag%'
    OR lower(COALESCE(url,''))   LIKE '%jaarbericht%'
    OR lower(COALESCE(url,''))   LIKE '%annual%report%'
    OR lower(COALESCE(url,''))   LIKE '%abtn%'
    OR lower(COALESCE(url,''))   LIKE '%beleggingsbeginselen%'
  )
  AND lower(COALESCE(title,'')) NOT LIKE '%verkort%'
"""

UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 pensioenfondsen-research"}
MAX_PER_FUND = 2
MAX_MB = 40
SLEEP_S = 1.5


def candidates(con, fund_id):
    rows = con.execute(
        f"SELECT title, url FROM scraped_documents "
        f"WHERE fund_id = ? AND {DOC_FILTER}", (fund_id,)).fetchall()
    scored = []
    for title, url in rows:
        if not url or not url.lower().startswith("http"):
            continue
        blob = f"{title or ''} {url}"
        yrs = [int(y) for y in re.findall(r"20\d\d", blob)]
        is_jv = bool(re.search(r"jaarverslag|jaarbericht|annual", blob.lower()))
        scored.append((is_jv, max(yrs) if yrs else 0, title or "", url))
    # jaarverslagen eerst, dan nieuwste jaar
    scored.sort(key=lambda t: (t[0], t[1]), reverse=True)
    seen, out = set(), []
    for is_jv, yr, title, url in scored:
        if url in seen:
            continue
        seen.add(url)
        out.append((yr, title, url))
    return out[:MAX_PER_FUND]


def download(fund_id, url):
    os.makedirs(CACHE_DIR, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9]+", "_", url.split("//", 1)[-1])[-80:]
    path = os.path.join(CACHE_DIR, f"{fund_id}_{safe}.pdf")
    if os.path.exists(path):
        return path, "cache"
    try:
        r = requests.get(url, headers=UA, timeout=30, stream=True)
        r.raise_for_status()
        if "pdf" not in r.headers.get("Content-Type", "").lower() \
           and not url.lower().endswith(".pdf"):
            return None, "geen pdf"
        size = 0
        with open(path, "wb") as f:
            for chunk in r.iter_content(1 << 16):
                size += len(chunk)
                if size > MAX_MB * (1 << 20):
                    f.close(); os.remove(path)
                    return None, f">{MAX_MB}MB"
                f.write(chunk)
        time.sleep(SLEEP_S)
        return path, "ok"
    except Exception as e:
        if os.path.exists(path):
            os.remove(path)
        return None, type(e).__name__


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--no-download", action="store_true",
                    help="alleen al-gedownloade PDF's gebruiken")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    targets = con.execute(
        "SELECT id, name FROM funds WHERE benchmark_providers IS NULL ORDER BY name"
    ).fetchall()

    plan, no_docs, failed, no_hit = [], [], [], []
    for fid, name in targets:
        cands = candidates(con, fid)
        if not cands:
            no_docs.append((fid, name))
            continue
        provs, src = [], None
        errs = []
        for yr, title, url in cands:
            if args.no_download:
                safe = re.sub(r"[^A-Za-z0-9]+", "_", url.split("//", 1)[-1])[-80:]
                path = os.path.join(CACHE_DIR, f"{fid}_{safe}.pdf")
                status = "cache" if os.path.exists(path) else "niet gecachet"
                path = path if os.path.exists(path) else None
            else:
                path, status = download(fid, url)
            if not path:
                errs.append(f"{status}: {url[:60]}")
                continue
            provs = providers_in(pdf_text(path))
            if provs:
                src = url
                break
        if provs:
            plan.append((fid, name, ", ".join(provs), src))
        elif errs and len(errs) == len(cands):
            failed.append((fid, name, errs[0]))
        else:
            no_hit.append((fid, name))

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"=== {mode} — fase 2: benchmark_providers via scraped_documents ===")
    print(f"NULL-fondsen: {len(targets)} | treffer: {len(plan)} | "
          f"docs maar geen treffer: {len(no_hit)} | download mislukt: {len(failed)} | "
          f"geen kandidaat-docs: {len(no_docs)}\n")

    for fid, name, val, src in plan:
        print(f"  id={fid:<4} {name[:42]:44s} -> {val}")
        print(f"          bron: {src[:90]}")
    if failed:
        print("\nDownload mislukt:")
        for fid, name, err in failed:
            print(f"  id={fid:<4} {name[:38]:40s} | {err}")
    if no_hit:
        print("\nDocs gescand, geen provider (blijft NULL):")
        for fid, name in no_hit:
            print(f"  id={fid:<4} {name[:60]}")
    if no_docs:
        print("\nGeen kandidaat-documenten in scraped_documents (blijft NULL):")
        for fid, name in no_docs:
            print(f"  id={fid:<4} {name[:60]}")

    if not args.apply:
        print("\nDry-run: er is NIETS naar de DB geschreven.")
        con.close()
        return

    cur = con.cursor()
    for fid, _, val, _ in plan:
        cur.execute("UPDATE funds SET benchmark_providers = ? "
                    "WHERE id = ? AND benchmark_providers IS NULL", (val, fid))
    con.commit()
    con.close()
    print(f"\nKlaar: {len(plan)} rijen geschreven (NULL-only).")


if __name__ == "__main__":
    main()
