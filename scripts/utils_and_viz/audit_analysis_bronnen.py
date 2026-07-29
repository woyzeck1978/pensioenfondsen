"""Controleer of elke jaarverslag-analyse op het jaarverslag van het júiste fonds rust.

Aanleiding: een analyse van Medisch Specialisten bleek geschreven op het
jaarverslag van een tbs-kliniek, ASR PPI op dat van het Nederlands Filmfonds en
Campina op dat van Koninklijke FrieslandCampina. De bestandsnaam begint met het
fonds-id, dus alles wat daarna misgaat is onzichtbaar: de parser leest gewoon
het verkeerde document en niets slaat alarm.

Per analyse worden drie dingen bekeken:

  naamtreffer  — komt een kenmerkend woord uit de fondsnaam voor op de eerste
                 pagina's van de PDF?
  gedeeld      — staat hetzelfde bestand (md5) onder meerdere fonds-ids?
  pensioen     — komt het woord 'pensioen' überhaupt voor?

Een bestand dat onder meerdere id's staat is niet meteen fout: APF- en
HNPF-kringen delen terecht één jaarverslag. Het wordt pas verdacht als de
fondsnaam niet in het document voorkomt terwijl die van een ander fonds dat wel
doet.

  python3 scripts/utils_and_viz/audit_analysis_bronnen.py
  python3 scripts/utils_and_viz/audit_analysis_bronnen.py --alleen-fout
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sqlite3
import sys
from collections import defaultdict

import fitz

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

# Woorden die in vrijwel elke fondsnaam voorkomen en dus niets onderscheiden.
GENERIEK = {
    "pensioenfonds", "pensioen", "stichting", "bedrijfstakpensioenfonds", "fonds",
    "van", "de", "het", "en", "voor", "nederland", "nederlandse", "bpf", "spf",
    "apf", "ppi", "kring", "beroepspensioenfonds", "personeel", "medewerkers",
}


def kenmerkend(naam: str) -> list[str]:
    """Woorden uit de fondsnaam waarop je een document kunt herkennen.

    Ook afkortingen van drie letters tellen mee: ABP, KPN, DOW, TNO en UWV
    houden na het strippen van het Engelse deel anders niets over, waardoor ze
    allemaal ten onrechte als verdacht zouden worden gemeld.
    """
    zonder_engels = re.sub(r"\([^)]*\)", " ", naam)          # '(Retail)' weg
    woorden = re.findall(r"[A-Za-zÀ-ÿ]{3,}", zonder_engels.lower())
    return [w for w in woorden if w not in GENERIEK]


def md5(pad: str) -> str:
    h = hashlib.md5()
    with open(pad, "rb") as f:
        for blok in iter(lambda: f.read(1 << 20), b""):
            h.update(blok)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--alleen-fout", action="store_true", help="verberg de goedgekeurde regels")
    ap.add_argument("--pagina-s", type=int, default=6, help="hoeveel pagina's er worden gelezen")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    rijen = con.execute("""
        SELECT fa.fund_id, f.name, fa.fiscal_year, fa.source_pdf
        FROM fund_analysis fa JOIN funds f ON f.id = fa.fund_id
        WHERE fa.source_pdf LIKE 'data/%'
        ORDER BY f.name
    """).fetchall()
    con.close()

    # Welke bestanden staan onder meerdere fonds-ids?
    per_hash = defaultdict(set)
    for _, _, _, src in rijen:
        vol = os.path.join(BASE_DIR, src)
        if os.path.exists(vol):
            per_hash[md5(vol)].add(src)

    fout, gedeeld_ok, ok = [], [], []
    for fid, naam, fy, src in rijen:
        vol = os.path.join(BASE_DIR, src)
        if not os.path.exists(vol):
            fout.append((fid, naam, fy, "bronbestand ontbreekt", os.path.basename(src)))
            continue
        try:
            doc = fitz.open(vol)
            tekst = " ".join(doc[i].get_text() for i in range(min(args.pagina_s, len(doc)))).lower()
            doc.close()
        except Exception:
            fout.append((fid, naam, fy, "bron onleesbaar", os.path.basename(src)))
            continue

        # Op woordgrens zoeken: 'dow' mag niet matchen op 'window' of 'dowarme'.
        treffers = [w for w in kenmerkend(naam)
                    if re.search(rf"\b{re.escape(w)}\b", tekst)]
        gedeeld = len(per_hash.get(md5(vol), set())) > 1
        if treffers:
            (gedeeld_ok if gedeeld else ok).append((fid, naam, fy, ",".join(treffers[:2]), os.path.basename(src)))
        elif "pensioen" not in tekst:
            fout.append((fid, naam, fy, "geen fondsnaam én geen 'pensioen'", os.path.basename(src)))
        else:
            fout.append((fid, naam, fy, "fondsnaam komt niet voor in de bron", os.path.basename(src)))

    print(f"{len(rijen)} analyses gecontroleerd — {len(fout)} verdacht, "
          f"{len(gedeeld_ok)} op een gedeeld maar passend bestand, {len(ok)} in orde\n")
    print("VERDACHT")
    for fid, naam, fy, reden, best in fout:
        print(f"  {fid:>4} {naam[:32]:<33} FY{fy}  {reden:<36} {best[:40]}")
    if not args.alleen_fout and gedeeld_ok:
        print("\nGEDEELD BESTAND, MAAR NAAM KOMT VOOR (meestal APF- of HNPF-kringen)")
        for fid, naam, fy, tref, best in gedeeld_ok:
            print(f"  {fid:>4} {naam[:32]:<33} FY{fy}  op '{tref}'{'':<12} {best[:40]}")
    return 1 if fout else 0


if __name__ == "__main__":
    sys.exit(main())
