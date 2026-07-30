"""Haal jaarverslagen op voor fondsen waarvan de analyse achterloopt.

Zoekt in scraped_documents naar een jaarverslag-URL van het gevraagde boekjaar,
haalt die op en keurt het resultaat vóór opslag. Dat laatste is de kern: eerder
belandden een 403-pagina van 206 bytes, het jaarverslag van het Nederlands
Filmfonds en het toezichtverslag van een tbs-kliniek ongemerkt in data/, omdat
de downloader alleen keek of er íets terugkwam.

Elke download moet door vier controles:
  1. HTTP 200 en een %PDF-header
  2. groter dan MIN_BYTES en meer dan één pagina
  3. het boekjaar staat op de omslag
  4. een kenmerkend woord uit de fondsnaam komt in de eerste pagina's voor,
     tenzij de PDF van het eigen domein van het fonds komt -- dat is een sterker
     bewijs van herkomst. Het verslag van SPMS heet 'Uw pensioen. Ons
     specialisme' en noemt de fondsnaam nergens vooraan, maar staat op spms.nl.

Zakt een bestand daarop af, dan wordt het niet opgeslagen maar gemeld.

  python3 scripts/data_collection/haal_jaarverslagen.py --jaar 2025 --max 10
  python3 scripts/data_collection/haal_jaarverslagen.py --jaar 2025 --fondsen 51,72
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
import urllib.parse
import urllib.request

import fitz

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")
DOEL_MAP = os.path.join(BASE_DIR, "data", "annual_reports")
MIN_BYTES = 200_000
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

GENERIEK = {"pensioenfonds", "pensioen", "stichting", "bedrijfstakpensioenfonds", "fonds",
            "van", "de", "het", "en", "voor", "nederland", "nederlandse", "bpf", "spf",
            "apf", "ppi", "kring", "beroepspensioenfonds", "personeel", "medewerkers"}


def kenmerkend(naam: str) -> list[str]:
    zonder = re.sub(r"\([^)]*\)", " ", naam)
    return [w for w in re.findall(r"[A-Za-zÀ-ÿ]{3,}", zonder.lower()) if w not in GENERIEK]


def kies_url(con, fund_id: int, jaar: int) -> str | None:
    """Beste jaarverslag-URL voor dit fonds en boekjaar; verkorte versies laatst."""
    kandidaten = [r[0] for r in con.execute(
        "SELECT url FROM scraped_documents WHERE fund_id = ? AND lower(url) LIKE '%.pdf'",
        (fund_id,))]
    treffers = [u for u in kandidaten
                if str(jaar) in u
                and re.search(r"jaarverslag|jaarbericht|jaarrapport|jv_", u, re.I)]
    if not treffers:
        return None
    # Verkort, MVB- en infographic-versies zijn geen bruikbare bron.
    def straf(u: str) -> tuple:
        slecht = bool(re.search(r"verkort|mvb|verantwoord|populair|infograph|in.?beeld", u, re.I))
        return (slecht, -len(u))
    return sorted(treffers, key=straf)[0]


def zelfde_domein(url: str, website: str | None) -> bool:
    """Komt de PDF van het eigen domein van het fonds?

    Dat is een sterker bewijs van herkomst dan een naamtreffer in de tekst: het
    jaarverslag van SPMS heet 'Uw pensioen. Ons specialisme' en noemt de
    fondsnaam nergens op de eerste pagina's, maar staat wel op spms.nl.
    """
    if not website:
        return False
    def kern(u: str) -> str:
        host = urllib.parse.urlparse(u).netloc.lower()
        return host[4:] if host.startswith("www.") else host
    return bool(kern(url)) and kern(url) == kern(website)


def keur(pad: str, jaar: int, naam: str, van_eigen_site: bool = False) -> str | None:
    """None als het bestand deugt, anders de reden waarom niet."""
    if os.path.getsize(pad) < MIN_BYTES:
        return f"te klein ({os.path.getsize(pad):,} bytes)"
    with open(pad, "rb") as f:
        if f.read(4) != b"%PDF":
            return "geen PDF-header"
    try:
        doc = fitz.open(pad)
        n = len(doc)
        tekst = re.sub(r"\s+", " ", " ".join(doc[i].get_text() for i in range(min(6, n)))).lower()
        doc.close()
    except Exception as e:
        return f"onleesbaar ({type(e).__name__})"
    if n <= 1:
        return "maar één pagina"
    if str(jaar) not in tekst:
        return f"boekjaar {jaar} staat niet op de eerste pagina's"
    woorden = kenmerkend(naam)
    if not van_eigen_site and woorden and not any(
            re.search(rf"\b{re.escape(w)}\b", tekst) for w in woorden):
        return f"fondsnaam komt niet voor (gezocht op {', '.join(woorden[:3])})"
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jaar", type=int, default=2025)
    ap.add_argument("--max", type=int, default=10, help="hoeveel fondsen deze run")
    ap.add_argument("--fondsen", type=str, default="", help="komma-gescheiden fonds-ids")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    if args.fondsen:
        ids = [int(x) for x in args.fondsen.split(",") if x.strip()]
        doelen = [(fid,) + con.execute(
            "SELECT name, COALESCE(aum_euro_bn,0), website FROM funds WHERE id=?",
            (fid,)).fetchone() for fid in ids]
    else:
        doelen = con.execute("""
            SELECT a.fund_id, f.name, COALESCE(f.aum_euro_bn, 0), f.website FROM fund_analysis a
            JOIN funds f ON f.id = a.fund_id
            WHERE a.fiscal_year = (SELECT MAX(fiscal_year) FROM fund_analysis WHERE fund_id = a.fund_id)
              AND a.fiscal_year < ? AND COALESCE(f.is_pensioenfonds, 1) = 1
            ORDER BY COALESCE(f.aum_euro_bn, 0) DESC LIMIT ?""", (args.jaar, args.max)).fetchall()

    os.makedirs(DOEL_MAP, exist_ok=True)
    goed = afgekeurd = geen_url = 0
    for fid, naam, aum, website in doelen:
        url = kies_url(con, fid, args.jaar)
        if not url:
            print(f"  {fid:>4} {naam[:34]:<35} geen {args.jaar}-URL bekend")
            geen_url += 1
            continue
        kort = re.sub(r"[^A-Za-z0-9]+", "_", naam.split("(")[0].strip())[:24].strip("_")
        pad = os.path.join(DOEL_MAP, f"{fid}_{kort}_{args.jaar}.pdf")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/pdf,*/*"})
            with urllib.request.urlopen(req, timeout=120) as r, open(pad, "wb") as f:
                f.write(r.read())
        except Exception as e:
            print(f"  {fid:>4} {naam[:34]:<35} download mislukt: {type(e).__name__}")
            afgekeurd += 1
            continue
        reden = keur(pad, args.jaar, naam, zelfde_domein(url, website))
        if reden:
            os.remove(pad)
            print(f"  {fid:>4} {naam[:34]:<35} AFGEKEURD: {reden}")
            afgekeurd += 1
        else:
            print(f"  {fid:>4} {naam[:34]:<35} ok  {os.path.getsize(pad)//1024} kB  {os.path.basename(pad)}")
            goed += 1
    print(f"\n{goed} opgehaald, {afgekeurd} afgekeurd, {geen_url} zonder bekende URL")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
