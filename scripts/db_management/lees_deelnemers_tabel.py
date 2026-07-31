"""Lees deelnemersaantallen uit de kerncijfertabel van het jaarverslag.

Waarom niet met een regex: die getallen staan in tabellen, en platgeslagen
PDF-tekst maakt daar "404 Totaal aantal deelnemers 6" van. PyMuPDF herkent de
tabel wel als structuur, met kopregel en kolommen, en dan is het eenvoudig.

Wat daarbij aan het licht kwam: de bestaande waarden in historical_metrics zijn
niet zomaar fout, ze komen uit de verkeerde kolom. Thales staat over 2025 op een
totaal van 6.377, en dat is exact de kolom van 2024; het verslag geeft 6.790 voor
2025. Een kerncijfertabel toont vijf jaargangen naast elkaar en wie de kop niet
leest, pakt de verkeerde.

Daarom leest dit script de kopregel en zoekt daarin het gevraagde jaartal, in
plaats van te vertrouwen op de volgorde.

  python3 scripts/db_management/lees_deelnemers_tabel.py --jaar 2025
  python3 scripts/db_management/lees_deelnemers_tabel.py --jaar 2025 --apply
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

# Rijlabels zoals ze in kerncijfertabellen voorkomen, per veld.
LABELS = {
    "actief": re.compile(r"^\s*(actieve? deelnemers|actieven|deelnemers actief)", re.I),
    "slapers": re.compile(r"^\s*(gewezen deelnemers|slapers|premievrije)", re.I),
    "gepensioneerd": re.compile(r"^\s*(pensioengerechtigden?|ingegane pensioenen|gepensioneerden)", re.I),
    "totaal": re.compile(r"^\s*totaal(\s+aantal)?\s+deelnemers", re.I),
}
# Een regel als '- waarvan arbeidsongeschikt' is een onderverdeling, geen categorie.
SUBRIJ = re.compile(r"^\s*[-•]|waarvan", re.I)


def _getal(cel) -> int | None:
    if cel is None:
        return None
    tekst = str(cel).strip().replace(".", "").replace(" ", "")
    if not re.fullmatch(r"-?\d{2,9}", tekst):
        return None
    n = int(tekst)
    return n if 10 <= n <= 3_000_000 else None


def uit_tabel(pdf_pad: str, jaar: int) -> dict[str, int] | None:
    """Deelnemersaantallen voor dit boekjaar, of None als de tabel niet te vinden is."""
    import fitz

    # Verslagen met een rommelige structuurboom laten MuPDF luid klagen op
    # stderr; die meldingen zeggen niets over de tabelherkenning en verdringen
    # de uitvoer.
    try:
        fitz.TOOLS.mupdf_display_errors(False)
    except Exception:
        pass

    pad = pdf_pad if os.path.isabs(pdf_pad) else os.path.join(BASE_DIR, pdf_pad)
    if not os.path.exists(pad):
        return None
    doc = fitz.open(pad)
    beste = None
    for bladzijde in doc:
        if "deelnemer" not in bladzijde.get_text().lower():
            continue
        try:
            tabellen = bladzijde.find_tables()
        except Exception:
            continue
        for tb in tabellen:
            rijen = tb.extract()
            if len(rijen) < 3:
                continue
            # In welke kolom staat het gevraagde jaar? De kop kan '2025' of
            # '31-12-2025' zijn.
            kolom = None
            for r in rijen[:3]:
                for i, cel in enumerate(r):
                    if cel and re.search(rf"\b{jaar}\b", str(cel)):
                        kolom = i
                        break
                if kolom is not None:
                    break
            if kolom is None:
                continue
            vondst: dict[str, int] = {}
            for r in rijen:
                label = str(r[0] or "")
                if SUBRIJ.search(label):
                    continue
                for veld, patroon in LABELS.items():
                    if veld in vondst or not patroon.search(label):
                        continue
                    if kolom < len(r):
                        n = _getal(r[kolom])
                        if n is not None:
                            vondst[veld] = n
            # De tabel met de meeste velden wint; een kerncijferoverzicht heeft ze alle vier.
            if vondst and (beste is None or len(vondst) > len(beste)):
                beste = vondst
    doc.close()
    return beste


def bron_voor(con, fid: int, jaar: int) -> str | None:
    r = con.execute("""SELECT pad FROM ophaal_wachtrij WHERE fund_id=? AND jaar=?
                       AND pad IS NOT NULL""", (fid, jaar)).fetchone()
    if r:
        return r[0]
    g = sorted(glob.glob(os.path.join(BASE_DIR, "data", "annual_reports", f"{fid}_*_{jaar}.pdf")))
    return os.path.relpath(g[0], BASE_DIR) if g else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jaar", type=int, default=2025)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--max", type=int, default=60)
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rijen = con.execute("""
        SELECT h.rowid rid, h.fund_id, f.name, h.deelnemers_actief a, h.deelnemers_slapers s,
               h.deelnemers_pensioengerechtigd g, h.deelnemers_totaal t
        FROM historical_metrics h JOIN funds f ON f.id = h.fund_id
        WHERE h.year = ? ORDER BY f.name""", (args.jaar,)).fetchall()

    voorstel, ongewijzigd, geen_bron = [], 0, 0
    for r in rijen[:args.max]:
        bron = bron_voor(con, r["fund_id"], args.jaar)
        if not bron:
            geen_bron += 1
            continue
        gelezen = uit_tabel(bron, args.jaar)
        if not gelezen or "totaal" not in gelezen:
            continue
        oud = {"actief": r["a"], "slapers": r["s"], "gepensioneerd": r["g"], "totaal": r["t"]}
        anders = {k: v for k, v in gelezen.items() if oud.get(k) != v}
        if anders:
            voorstel.append((r, bron, gelezen, anders))
        else:
            ongewijzigd += 1

    print(f"{len(rijen)} rijen over {args.jaar}; {geen_bron} zonder verslag, "
          f"{ongewijzigd} al gelijk aan het verslag\n")
    print(f"{len(voorstel)} rijen wijken af van hun eigen jaarverslag:\n")
    for r, bron, gelezen, anders in voorstel:
        oud = f"{r['a']}/{r['s']}/{r['g']}/{r['t']}"
        nieuw = "/".join(str(gelezen.get(k, "—")) for k in
                         ("actief", "slapers", "gepensioneerd", "totaal"))
        print(f"  {r['name'][:32]:<34} {oud:<28} -> {nieuw}")

    if not args.apply:
        print("\nDroogloop. Draai met --apply om dit weg te schrijven.")
        con.close()
        return 0

    kopie = DB_PATH.replace(".db", f".backup-{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
    shutil.copy2(DB_PATH, kopie)
    print(f"\nBack-up: {os.path.basename(kopie)}")
    kolom = {"actief": "deelnemers_actief", "slapers": "deelnemers_slapers",
             "gepensioneerd": "deelnemers_pensioengerechtigd", "totaal": "deelnemers_totaal"}
    for r, _bron, gelezen, _anders in voorstel:
        for veld, waarde in gelezen.items():
            con.execute(f"UPDATE historical_metrics SET {kolom[veld]} = ? WHERE rowid = ?",
                        (waarde, r["rid"]))
    con.commit()
    print(f"{len(voorstel)} rijen hersteld uit het jaarverslag.")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
