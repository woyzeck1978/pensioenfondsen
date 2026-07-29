"""Zet funds.aum_euro_bn gelijk aan het nieuwste jaar in historical_metrics.

Waarom: funds.aum_euro_bn heeft geen vast peiljaar. Bij 64 fondsen komt de
waarde overeen met FY2025, bij 44 met FY2024, en bij een handvol met een jaar
uit 2015-2022. De diepteanalyse zet die waarde bovenaan als KPI en het
boekjaarcijfer eronder in het tabblad Kerncijfers, wat bij 43 fondsen een
verschil van meer dan 5% oplevert dat op een fout lijkt maar een jaarverschil is.

Alleen AUM. De dekkingsgraden blijven met rust: die zijn in de fondsentabel
bedoeld als actuele stand, niet als jaareinde, en wijken maar bij één fonds af.

Standaard is dit een droogloop. Pas met --apply wordt er geschreven, en dan
wordt eerst een kopie van de DB weggezet.

  python3 scripts/db_management/sync_aum_from_historical.py
  python3 scripts/db_management/sync_aum_from_historical.py --apply
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
DREMPEL_REL = 0.005  # onder 0,5% verschil is het afronding, niet het moeite waard


def voorstellen(con):
    """[(fund_id, naam, oud, nieuw, jaar, oud_jaar)] voor alles wat afwijkt."""
    rijen = con.execute("""
        SELECT f.id, f.name, f.aum_euro_bn, h.year, h.aum_euro_bn
        FROM funds f
        JOIN historical_metrics h ON h.fund_id = f.id
        WHERE h.aum_euro_bn IS NOT NULL AND h.aum_euro_bn > 0
          AND h.year = (SELECT MAX(year) FROM historical_metrics
                        WHERE fund_id = f.id AND aum_euro_bn IS NOT NULL AND aum_euro_bn > 0)
        ORDER BY h.aum_euro_bn DESC
    """).fetchall()

    # Bij welk jaar hoorde de oude waarde? Dat maakt zichtbaar of we een fonds
    # één jaar bijwerken of tien.
    per_fonds = {}
    for fid, jaar, aum in con.execute(
            "SELECT fund_id, year, aum_euro_bn FROM historical_metrics "
            "WHERE aum_euro_bn IS NOT NULL AND aum_euro_bn > 0"):
        per_fonds.setdefault(fid, []).append((jaar, aum))

    uit = []
    for fid, naam, oud, jaar, nieuw in rijen:
        if oud is None or oud <= 0:
            uit.append((fid, naam, oud, nieuw, jaar, None))
            continue
        if abs(oud - nieuw) / nieuw <= DREMPEL_REL:
            continue
        oud_jaar = None
        for j, a in sorted(per_fonds.get(fid, []), reverse=True):
            if abs(a - oud) / oud < 0.02:
                oud_jaar = j
                break
        uit.append((fid, naam, oud, nieuw, jaar, oud_jaar))
    return uit


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="schrijf de wijzigingen weg")
    ap.add_argument("--max-sprong", type=float, default=0.25,
                    help="relatieve wijziging waarboven een fonds met rust wordt gelaten")
    ap.add_argument("--ook-grote-sprongen", action="store_true",
                    help="pas ook de fondsen boven --max-sprong toe")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    alles = voorstellen(con)

    # Een AUM die met een kwart of meer verspringt is zelden een jaarverschil.
    # Loodsen zou van 1,00 naar 0,12 mrd gaan en Huisartsen van 12,50 naar 9,26;
    # dat moet je tegen het jaarverslag houden voor je het overneemt.
    groot = [v for v in alles
             if v[2] and abs(v[3] - v[2]) / v[2] > args.max_sprong]
    voorstel = alles if args.ook_grote_sprongen else [v for v in alles if v not in groot]

    if not alles:
        print("Niets te synchroniseren.")
        con.close()
        return 0

    if groot and not args.ook_grote_sprongen:
        print(f"{len(groot)} fondsen springen meer dan "
              f"{args.max_sprong * 100:.0f}% — overgeslagen, controleer met de hand:\n")
        for _, naam, oud, nieuw, jaar, oud_jaar in groot:
            print(f"  {naam[:43]:<44} {oud:>8,.2f} → {nieuw:>8,.2f}  "
                  f"(FY{oud_jaar or '?'} → FY{jaar})")
        print()

    print(f"{len(voorstel)} fondsen wijken af van hun nieuwste boekjaar\n")
    print(f"{'fonds':<44} {'nu':>9} {'wordt':>9}  {'van':>7} {'naar':>6}")
    print("-" * 82)
    for _, naam, oud, nieuw, jaar, oud_jaar in voorstel:
        oud_s = f"{oud:,.2f}" if oud is not None else "leeg"
        vanaf = f"FY{oud_jaar}" if oud_jaar else "?"
        print(f"{naam[:43]:<44} {oud_s:>9} {nieuw:>9,.2f}  {vanaf:>7} {'FY' + str(jaar):>6}")

    if not args.apply:
        print("\nDroogloop. Draai met --apply om dit weg te schrijven.")
        con.close()
        return 0

    kopie = DB_PATH.replace(".db", f".backup-{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
    shutil.copy2(DB_PATH, kopie)
    print(f"\nBack-up: {os.path.basename(kopie)}")

    for fid, _, _, nieuw, jaar, _ in voorstel:
        con.execute("UPDATE funds SET aum_euro_bn = ?, annual_report_year = ? WHERE id = ?",
                    (nieuw, jaar, fid))
    con.commit()
    print(f"{len(voorstel)} fondsen bijgewerkt; annual_report_year meteen op het juiste jaar gezet.")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
