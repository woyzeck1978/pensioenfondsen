"""Vul lege kernvelden uit het jaarverslag dat al op schijf staat.

De fondsentabel heeft gaten: bij tientallen fondsen ontbreekt de dekkingsgraad of
het deelnemersaantal, terwijl het verslag waaruit die te lezen zijn er gewoon
ligt. Dit script haalt ze eruit.

Twee regels die het onderscheid maken met een gewone scraper:

  1. Alleen vullen, nooit overschrijven. Staat er al iets, dan wordt hooguit een
     afwijking gemeld. Bij Chemours bleek de bestaande waarde de DNB-stand van
     2026Q1 te zijn — recenter dan het jaarverslag, dus overschrijven zou de
     gegevens ouder maken.
  2. Alleen als het patroon eenduidig is. Levert een fonds twee verschillende
     waarden op, dan wordt niets ingevuld maar de keuze voorgelegd. Een half
     ingevuld veld is beter dan een verkeerd ingevuld veld.

  python3 scripts/db_management/vul_kernvelden.py
  python3 scripts/db_management/vul_kernvelden.py --apply
  python3 scripts/db_management/vul_kernvelden.py --veld deelnemers --apply
"""

from __future__ import annotations

import argparse
import glob
import os
import re
import shutil
import sqlite3
import sys
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

# Een dekkingsgraad buiten dit bereik is geen dekkingsgraad maar een leesfout.
DGR_MIN, DGR_MAX = 60.0, 300.0
# Idem voor deelnemers: onder de honderd is het een tabelcel, boven de drie
# miljoen bestaat geen Nederlands pensioenfonds.
DEELNEMERS_MIN, DEELNEMERS_MAX = 100, 3_000_000

DEKKINGSGRAAD = [
    re.compile(r"actuele dekkingsgraad[^.]{0,60}?ultimo 2025[^.]{0,30}?(\d{2,3},\d)\s?%", re.I),
    re.compile(r"dekkingsgraad per einde boekjaar[^.]{0,40}?(\d{2,3},\d)\s?%", re.I),
    re.compile(r"eind 2025 (?:was|bedroeg) de (?:actuele )?dekkingsgraad\s*(\d{2,3},\d)\s?%", re.I),
    re.compile(r"(?:de )?actuele dekkingsgraad (?:is |was |bedroeg |bedraagt )"
               r"(?:in 2025 )?(?:gestegen |gedaald )?(?:naar |op )?(\d{2,3},\d)\s?%", re.I),
]
DEELNEMERS = [
    re.compile(r"totaal aantal deelnemers(?: en pensioengerechtigden)?\s*"
               r"([\d]{1,3}(?:\.\d{3})+|\d{3,7})", re.I),
    re.compile(r"aantal (?:verzekerden|deelnemers) totaal\s*([\d]{1,3}(?:\.\d{3})+|\d{3,7})", re.I),
]


def _getal(tekst: str) -> int:
    return int(tekst.replace(".", ""))


def lees(pdf_pad: str) -> tuple[float | None, int | None, list[str]]:
    """Dekkingsgraad en deelnemersaantal uit het verslag, plus wat er misging."""
    import fitz

    pad = pdf_pad if os.path.isabs(pdf_pad) else os.path.join(BASE_DIR, pdf_pad)
    if not os.path.exists(pad):
        return None, None, ["bestand ontbreekt"]
    try:
        doc = fitz.open(pad)
        tekst = re.sub(r"\s+", " ", " ".join(p.get_text() for p in doc))
        doc.close()
    except Exception as e:
        return None, None, [f"onleesbaar ({type(e).__name__})"]

    opmerkingen: list[str] = []

    dgr_waarden = {float(m.group(1).replace(",", ".")) for p in DEKKINGSGRAAD
                   for m in p.finditer(tekst)}
    dgr_waarden = {w for w in dgr_waarden if DGR_MIN <= w <= DGR_MAX}
    dgr = dgr_waarden.pop() if len(dgr_waarden) == 1 else None
    if len(dgr_waarden) > 1 or (dgr is None and dgr_waarden):
        opmerkingen.append("dekkingsgraad niet eenduidig: "
                           + ", ".join(f"{w:.1f}" for w in sorted(dgr_waarden)))

    dn_waarden = {_getal(m.group(1)) for p in DEELNEMERS for m in p.finditer(tekst)}
    dn_waarden = {w for w in dn_waarden if DEELNEMERS_MIN <= w <= DEELNEMERS_MAX}
    dn = dn_waarden.pop() if len(dn_waarden) == 1 else None
    if len(dn_waarden) > 1 or (dn is None and dn_waarden):
        opmerkingen.append("deelnemers niet eenduidig: "
                           + ", ".join(f"{w:,}".replace(",", ".") for w in sorted(dn_waarden)))
    return dgr, dn, opmerkingen


def bron_voor(con, fid: int) -> str | None:
    r = con.execute("""SELECT pad FROM ophaal_wachtrij WHERE fund_id=? AND jaar=2025
                       AND pad IS NOT NULL""", (fid,)).fetchone()
    if r:
        return r[0]
    for jaar in (2025, 2024):
        g = sorted(glob.glob(os.path.join(BASE_DIR, "data", "annual_reports", f"{fid}_*_{jaar}.pdf")))
        if g:
            return os.path.relpath(g[0], BASE_DIR)
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="schrijf de gevonden waarden weg")
    ap.add_argument("--veld", choices=["dekkingsgraad", "deelnemers"],
                    help="beperk tot dit veld")
    ap.add_argument("--max", type=int, default=40, help="hoeveel fondsen deze run")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    rijen = con.execute("""
        SELECT id, name, dekkingsgraad_pct, deelnemers_totaal FROM funds
        WHERE COALESCE(is_pensioenfonds, 1) = 1
          AND (dekkingsgraad_pct IS NULL OR deelnemers_totaal IS NULL)
        ORDER BY COALESCE(aum_euro_bn, 0) DESC""").fetchall()

    voorstel, gemeld, zonder = [], [], 0
    for fid, naam, dg, dn in rijen[:args.max]:
        bron = bron_voor(con, fid)
        if not bron:
            zonder += 1
            continue
        gevonden_dg, gevonden_dn, opm = lees(bron)
        acties = []
        if dg is None and gevonden_dg is not None and args.veld != "deelnemers":
            acties.append(("dekkingsgraad_pct", gevonden_dg))
        if dn is None and gevonden_dn is not None and args.veld != "dekkingsgraad":
            acties.append(("deelnemers_totaal", gevonden_dn))
        if acties:
            voorstel.append((fid, naam, bron, acties))
        if opm:
            gemeld.append((fid, naam, opm))

    print(f"{len(rijen)} fondsen met een leeg kernveld, {zonder} zonder verslag op schijf\n")
    print(f"{len(voorstel)} fondsen kunnen worden aangevuld:")
    for fid, naam, bron, acties in voorstel:
        wat = ", ".join(f"{k.replace('_pct','').replace('_totaal','')}={v}" for k, v in acties)
        print(f"  {fid:>4} {naam[:34]:<35} {wat:<34} {os.path.basename(bron)[:30]}")

    if gemeld:
        print(f"\n{len(gemeld)} fondsen leverden meerdere waarden op en worden overgeslagen:")
        for fid, naam, opm in gemeld[:12]:
            print(f"  {fid:>4} {naam[:32]:<33} {'; '.join(opm)[:88]}")

    if not args.apply:
        print("\nDroogloop. Draai met --apply om dit weg te schrijven.")
        con.close()
        return 0

    kopie = DB_PATH.replace(".db", f".backup-{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
    shutil.copy2(DB_PATH, kopie)
    print(f"\nBack-up: {os.path.basename(kopie)}")
    for fid, _naam, _bron, acties in voorstel:
        for kolom, waarde in acties:
            con.execute(f"UPDATE funds SET {kolom} = ? WHERE id = ? AND {kolom} IS NULL",
                        (waarde, fid))
    con.commit()
    print(f"{len(voorstel)} fondsen bijgewerkt.")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
