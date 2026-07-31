"""Toon de kerncijfers uit een briefing, compact genoeg om er een analyse uit te schrijven.

De briefings in data/interim/kern/ zijn per onderwerp gegroepeerd maar nog altijd
honderd regels lang. Dit script haalt daar de regels uit die een cijfer bevatten
én over een kerngrootheid gaan, en toont die per fonds op een handvol regels.

Zit het gezochte getal in een zin die langer is dan de afkapgrens van de
briefing, dan wordt teruggevallen op de bron-PDF zelf — bij HiBiN stond de
beleidsdekkingsgraad in zo'n lange zin en ontbrak hij daardoor in de briefing.

  python3 scripts/utils_and_viz/toon_briefing.py 165 20 115
  python3 scripts/utils_and_viz/toon_briefing.py --open      # alles wat nog wacht
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")
KERN_MAP = os.path.join(BASE_DIR, "data", "interim", "kern")

# Wat een analyse nodig heeft, in volgorde van belang.
VRAGEN = [
    ("dekkingsgraad", r"(actuele|beleids|re[eë]le|vereiste) ?dekkingsgraad"),
    ("rendement", r"(totaal)?rendement|beleggingsresultaat|benchmark|outperformance"),
    ("toeslag", r"toeslag|indexatie|verhoogd met|korting|verlaging"),
    ("transitie", r"invaren|invaardatum|transitiedatum|uitgesteld|solidaire premieregeling"
                  r"|flexibele premieregeling|solidariteitsreserve|\bDNB\b"),
    ("omvang", r"belegd vermogen|deelnemers|gepensioneerden|miljard|miljoen euro"),
    ("kosten", r"kosten per deelnemer|uitvoeringskosten|vermogensbeheerkosten"),
]
HEEFT_GETAL = re.compile(r"\d")
PROCENT = re.compile(r"\d{1,3},\d\s?%")


def uit_pdf(pad: str, patroon: str, hoogstens: int = 4) -> list[str]:
    """Terugval op de bron als de briefing het getal niet bevat."""
    try:
        import fitz
    except ImportError:
        return []
    vol = os.path.join(BASE_DIR, pad) if not os.path.isabs(pad) else pad
    if not os.path.exists(vol):
        return []
    doc = fitz.open(vol)
    tekst = re.sub(r"\s+", " ", " ".join(p.get_text() for p in doc))
    doc.close()
    uit, gezien = [], set()
    for m in re.finditer(rf"[^.]{{0,100}}{patroon}[^.]{{0,100}}\.", tekst, re.I):
        z = m.group(0).strip()
        if not PROCENT.search(z) or z[:40] in gezien:
            continue
        gezien.add(z[:40])
        uit.append(z)
        if len(uit) >= hoogstens:
            break
    return uit


def toon(con, fid: int, jaar: int) -> None:
    naam, = con.execute("SELECT name FROM funds WHERE id=?", (fid,)).fetchone() or ("?",)
    pad_rij = con.execute("SELECT pad FROM ophaal_wachtrij WHERE fund_id=? AND jaar=?",
                          (fid, jaar)).fetchone()
    bron = pad_rij[0] if pad_rij else None
    kern = os.path.join(KERN_MAP, f"{fid}_{jaar}.md")
    print(f"\n{'=' * 78}\n{fid}  {naam}   [{bron or 'geen bron'}]")
    if not os.path.exists(kern):
        print("  geen briefing")
        return
    regels = [r[2:] for r in open(kern).read().split("\n") if r.startswith("- ")]

    for kop, patroon in VRAGEN:
        pat = re.compile(patroon, re.I)
        gekozen, gezien = [], set()
        for r in regels:
            if pat.search(r) and HEEFT_GETAL.search(r) and r[:40] not in gezien:
                gezien.add(r[:40])
                gekozen.append(r)
            if len(gekozen) >= 5:
                break
        # Niets met een percentage gevonden? Dan de bron zelf raadplegen.
        if kop == "dekkingsgraad" and bron and not any(PROCENT.search(g) for g in gekozen):
            gekozen = uit_pdf(bron, r"dekkingsgraad") or gekozen
        if gekozen:
            print(f"  -- {kop}")
            for g in gekozen:
                print(f"     {g[:165]}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("fondsen", nargs="*", type=int)
    ap.add_argument("--jaar", type=int, default=2025)
    ap.add_argument("--open", action="store_true",
                    help="alle fondsen met status 'binnen' zonder analyse")
    ap.add_argument("--max", type=int, default=6)
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    ids = args.fondsen
    if args.open or not ids:
        ids = [r[0] for r in con.execute("""
            SELECT w.fund_id FROM ophaal_wachtrij w JOIN funds f ON f.id = w.fund_id
            WHERE w.jaar=? AND w.status='binnen'
              AND NOT EXISTS (SELECT 1 FROM fund_analysis a
                              WHERE a.fund_id=w.fund_id AND a.fiscal_year=w.jaar)
            ORDER BY COALESCE(f.aum_euro_bn,0) DESC LIMIT ?""", (args.jaar, args.max))]
    for fid in ids:
        toon(con, fid, args.jaar)
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
