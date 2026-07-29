"""Controleer de PDF-mappen op bestanden die geen bruikbaar document zijn.

Aanleiding: vijf fondsen hadden jarenlang een 'Access Denied'-pagina van 206
bytes als jaarverslag op schijf staan. De downloadscripts schreven weg wat de
server teruggaf zonder te kijken wát dat was, en niets verderop in de pipeline
merkte het — de parsers sloegen die fondsen simpelweg over.

Wat er wordt gemeld:
  - geen %PDF-header (meestal een HTML-foutpagina)
  - kleiner dan MIN_BYTES
  - onleesbaar voor PyMuPDF
  - één pagina met blokkeer-tekst erin (WAF-pagina die wél een PDF is)

Gebruik:
  python3 scripts/utils_and_viz/audit_pdfs.py              # alleen rapporteren
  python3 scripts/utils_and_viz/audit_pdfs.py --quarantine # verplaatsen

--quarantine verplaatst naar data/_broken/<map>/ en verwijdert dus niets;
terugzetten is een mv. Draaien vanuit de projectroot of waar dan ook: de paden
worden vanaf __file__ bepaald.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sys

import fitz

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DIRS = ["data/annual_reports", "data/reports", "data/historical_reports",
        "data/transitieplannen"]
QUARANTINE = "data/_broken"
MIN_BYTES = 20_000

# Tekst die een blokkeerpagina verraadt als die tóch als PDF is geserveerd.
BLOKKADE = re.compile(
    r"access denied|forbidden|403 error|challenge validation|"
    r"you don't have permission|request blocked", re.I)


def keur(pad: str):
    """(ok, reden). ok=False betekent: dit bestand is geen bruikbaar document."""
    grootte = os.path.getsize(pad)
    with open(pad, "rb") as f:
        kop = f.read(4)
    if kop != b"%PDF":
        return False, f"geen PDF-header ({kop!r})"
    if grootte < MIN_BYTES:
        return False, f"te klein ({grootte:,} bytes)"
    try:
        doc = fitz.open(pad)
        n = len(doc)
        tekst = doc[0].get_text() if n else ""
        doc.close()
    except Exception as e:
        return False, f"onleesbaar ({type(e).__name__})"
    if n <= 1 and BLOKKADE.search(tekst):
        return False, "blokkeerpagina (1 pagina)"
    if n == 0:
        return False, "nul pagina's"
    return True, ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quarantine", action="store_true",
                    help="verplaats de gevonden bestanden naar data/_broken/")
    args = ap.parse_args()

    stuk = []
    totaal = 0
    for d in DIRS:
        vol = os.path.join(BASE_DIR, d)
        if not os.path.isdir(vol):
            continue
        for naam in sorted(os.listdir(vol)):
            if not naam.lower().endswith(".pdf"):
                continue
            totaal += 1
            pad = os.path.join(vol, naam)
            ok, reden = keur(pad)
            if not ok:
                stuk.append((d, naam, reden, os.path.getsize(pad)))

    print(f"{totaal} PDF's gecontroleerd in {len(DIRS)} mappen — {len(stuk)} onbruikbaar\n")
    for d, naam, reden, grootte in stuk:
        print(f"  {reden:<28} {grootte:>10,} b  {d}/{naam}")

    if stuk and args.quarantine:
        print()
        for d, naam, _, _ in stuk:
            doel_map = os.path.join(BASE_DIR, QUARANTINE, os.path.basename(d))
            os.makedirs(doel_map, exist_ok=True)
            shutil.move(os.path.join(BASE_DIR, d, naam), os.path.join(doel_map, naam))
            print(f"  verplaatst → {QUARANTINE}/{os.path.basename(d)}/{naam}")
    elif stuk:
        print("\nDraai met --quarantine om ze naar data/_broken/ te verplaatsen.")

    return 1 if stuk else 0


if __name__ == "__main__":
    sys.exit(main())
