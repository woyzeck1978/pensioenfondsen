"""Zet funds.beleidsdekkingsgraad_pct gelijk aan de laatste DNB-kwartaalstand.

Net als bij de AUM loopt dit veld achter: bij twintig fondsen wijkt het meer dan
5 procentpunt af van wat DNB registreert, en bij HAL zelfs 124 punten (133,4%
tegen 257,6%). De DNB-kwartaalcijfers zijn door de fondsen zelf gerapporteerd aan
de toezichthouder en zijn daarmee de hardste bron die we hebben; in de meeste
gevallen komt historical_metrics er ook exact mee overeen.

Anders dan bij de AUM-synchronisatie zit hier geen bovengrens op de wijziging:
juist de grote sprongen zijn de fouten die we willen herstellen. Wel wordt elke
wijziging boven de 25 procentpunt apart gemarkeerd zodat je die kunt nalopen.

  python3 scripts/db_management/sync_beleidsdekkingsgraad_dnb.py
  python3 scripts/db_management/sync_beleidsdekkingsgraad_dnb.py --apply
"""

from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
import sys
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")
DREMPEL_PP = 5.0      # onder 5 procentpunt is het geen achterstand maar een peilmoment
OPVALLEND_PP = 25.0   # hierboven apart markeren

# Alleen bijwerken als DNB HOGER staat dan de fondsentabel. Bij 123 fondsen wijkt
# het veld meer dan 1 procentpunt af, maar in verreweg de meeste gevallen ligt
# funds juist bóven de DNB-stand van 2026Q1, en die weer boven het boekjaar 2025.
# Dat is de volgorde die past bij een stijgende reeks: het veld is daar actueler
# dan DNB, niet verouderd. Overschrijven zou de gegevens ouder maken. Alleen waar
# de fondsentabel fors lager staat, is de waarde blijven hangen — HAL stond op
# 133,4% terwijl DNB 257,6% registreert.
ALLEEN_ALS_DNB_HOGER = True


def voorstellen(con):
    laatste = {}
    for fid, waarde, jaar, kw in con.execute("""
            SELECT fund_id, value, year, quarter FROM dnb_quarterly_metrics
            WHERE metric_name LIKE 'Beleidsdekkingsgraad%' AND value IS NOT NULL
            ORDER BY fund_id, year, quarter"""):
        laatste[fid] = (waarde, jaar, kw)

    uit = []
    for fid, naam, huidig in con.execute(
            "SELECT id, name, beleidsdekkingsgraad_pct FROM funds ORDER BY name"):
        if fid not in laatste:
            continue
        nieuw, jaar, kw = laatste[fid]
        if huidig is not None and abs(huidig - nieuw) <= DREMPEL_PP:
            continue
        if ALLEEN_ALS_DNB_HOGER and huidig is not None and nieuw < huidig:
            continue
        # Wat zegt de eigen jaarrekening? Handig om naast de DNB-stand te leggen.
        hist = con.execute("""SELECT year, beleidsdekkingsgraad_pct FROM historical_metrics
            WHERE fund_id = ? AND beleidsdekkingsgraad_pct IS NOT NULL
            ORDER BY year DESC LIMIT 1""", (fid,)).fetchone()
        # DNB-cijfer ouder dan onze eigen reeks? Dan zou bijwerken de gegevens juist
        # terugzetten in de tijd. Metro is zo'n geval: DNB heeft 2023Q4, wij 2024.
        if hist and jaar < hist[0]:
            continue
        uit.append((fid, naam, huidig, nieuw, f"{jaar}Q{kw}", hist))
    return uit


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="schrijf de wijzigingen weg")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    voorstel = voorstellen(con)
    if not voorstel:
        print("Niets te synchroniseren.")
        con.close()
        return 0

    groot = [v for v in voorstel if v[2] is not None and abs(v[2] - v[3]) > OPVALLEND_PP]
    print(f"{len(voorstel)} fondsen wijken meer dan {DREMPEL_PP:g} procentpunt af van DNB\n")
    print(f"{'fonds':<32} {'nu':>7} {'DNB':>7} {'kwartaal':>9}  eigen jaarrekening")
    print("-" * 88)
    for fid, naam, huidig, nieuw, kw, hist in voorstel:
        nu = f"{huidig:.1f}" if huidig is not None else "leeg"
        h = f"{hist[1]:.1f} (FY{hist[0]})" if hist else "—"
        vlag = "  <<" if (huidig is not None and abs(huidig - nieuw) > OPVALLEND_PP) else ""
        print(f"{naam[:31]:<32} {nu:>7} {nieuw:>7.1f} {kw:>9}  {h}{vlag}")

    if groot:
        print(f"\n{len(groot)} fondsen springen meer dan {OPVALLEND_PP:g} procentpunt (<<) — nalopen tegen het jaarverslag.")

    if not args.apply:
        print("\nDroogloop. Draai met --apply om dit weg te schrijven.")
        con.close()
        return 0

    kopie = DB_PATH.replace(".db", f".backup-{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
    shutil.copy2(DB_PATH, kopie)
    print(f"\nBack-up: {os.path.basename(kopie)}")
    for fid, _, _, nieuw, _, _ in voorstel:
        con.execute("UPDATE funds SET beleidsdekkingsgraad_pct = ? WHERE id = ?", (nieuw, fid))
    con.commit()
    print(f"{len(voorstel)} fondsen bijgewerkt.")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
