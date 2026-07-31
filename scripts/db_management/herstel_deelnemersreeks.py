"""Maak de kapotte deelnemersaantallen in historical_metrics leeg en herstel wat kan.

De jaarreeks was nooit gecontroleerd terwijl de fondsentabel dat wel was. Bij een
eerste telling leek er van alles mis, maar dat kwam voor een groot deel door een
te grove controle. Wat er werkelijk aan de hand is, valt in vier soorten:

  onderdelen gekopieerd  Hetzelfde drietal actief/slapers/gepensioneerd staat bij
                         twee of meer fondsen. Arcadis, CK1 en CRH delen er een,
                         en die drie kringen hebben aantoonbaar verschillende
                         bestanden. Voor minstens twee ervan is het dus onjuist,
                         en welke de echte is valt niet af te leiden.
                         -> onderdelen leegmaken bij alle kopieën

  totaal ver mis         Het totaal wijkt meer dan de helft af van de som van de
                         onderdelen. PGB staat op 31 deelnemers terwijl de
                         onderdelen 130.638 geven; het totaal is dan het foute
                         getal, niet de uitsplitsing.
                         -> alleen het totaal leegmaken

  kleine afwijking       Enkele procenten verschil. Bij PMT is dat +4,5%, wat
                         past bij een categorie die niet in de drie kolommen zit
                         (arbeidsongeschikten bijvoorbeeld). Geen fout.
                         -> laten staan

  onvolledig             Een van de onderdelen is leeg, waardoor de som lager
                         uitkomt dan het totaal. Ook geen fout.
                         -> laten staan

  python3 scripts/db_management/herstel_deelnemersreeks.py
  python3 scripts/db_management/herstel_deelnemersreeks.py --apply
"""

from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

# Onder deze afwijking is het een categorieverschil, geen fout.
DREMPEL_KLEIN = 0.10
# Hierboven is niet de uitsplitsing verdacht maar het totaal.
DREMPEL_GROOT = 0.50


def classificeer(con) -> dict[str, list]:
    con.row_factory = sqlite3.Row
    rijen = con.execute("""
        SELECT h.rowid rid, h.fund_id, f.name, h.year,
               h.deelnemers_actief a, h.deelnemers_slapers s,
               h.deelnemers_pensioengerechtigd g, h.deelnemers_totaal t
        FROM historical_metrics h JOIN funds f ON f.id = h.fund_id
        WHERE h.deelnemers_totaal > 0""").fetchall()

    # Een drietal dat bij meerdere fondsen voorkomt, is bij hooguit een ervan echt.
    per_drietal = defaultdict(set)
    for r in rijen:
        if r["a"] and r["s"] and r["g"]:
            per_drietal[(r["a"], r["s"], r["g"])].add(r["fund_id"])
    gedeeld = {k for k, v in per_drietal.items() if len(v) > 1}

    uit = {"gekopieerd": [], "totaal_mis": [], "klein": [], "onvolledig": []}
    for r in rijen:
        compleet = all(r[k] is not None for k in ("a", "s", "g"))
        som = (r["a"] or 0) + (r["s"] or 0) + (r["g"] or 0)
        if not som:
            continue
        afwijking = abs(r["t"] - som) / r["t"]
        if afwijking <= 0.01:
            continue
        if compleet and (r["a"], r["s"], r["g"]) in gedeeld:
            uit["gekopieerd"].append(r)
        elif not compleet:
            uit["onvolledig"].append(r)
        # De richting is bepalend. Onderdelen die samen bóven het totaal uitkomen
        # kan niet: actief, slapers en gepensioneerden sluiten elkaar uit. Een
        # totaal dat hóger ligt dan de som kan juist prima — dan zit er een
        # categorie in die niet in de drie kolommen staat, zoals
        # arbeidsongeschikten. PGB heeft over 2021 een totaal van 342.150 bij een
        # som van 219.487; dat leegmaken zou de goede waarde weggooien.
        elif som > r["t"] * (1 + DREMPEL_KLEIN):
            uit["totaal_mis"].append(r)
        elif r["t"] > som * (1 + DREMPEL_GROOT) * 2:
            uit["totaal_mis"].append(r)
        else:
            uit["klein"].append(r)
    return uit


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="schrijf de wijzigingen weg")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    groepen = classificeer(con)

    print(f"{len(groepen['gekopieerd'])} rijen met gekopieerde onderdelen "
          f"— uitsplitsing wordt leeggemaakt\n")
    for r in sorted(groepen["gekopieerd"], key=lambda x: (x["name"], x["year"])):
        print(f"  {r['name'][:34]:<36} FY{r['year']}  {r['a']}/{r['s']}/{r['g']}")

    print(f"\n{len(groepen['totaal_mis'])} rijen met een totaal dat niet bij de "
          f"uitsplitsing past — totaal wordt leeggemaakt\n")
    for r in sorted(groepen["totaal_mis"], key=lambda x: (x["name"], x["year"])):
        som = (r["a"] or 0) + (r["s"] or 0) + (r["g"] or 0)
        print(f"  {r['name'][:34]:<36} FY{r['year']}  som {som:>9} vs totaal {r['t']:>9}")

    print(f"\nBlijven staan: {len(groepen['klein'])} met een klein categorieverschil, "
          f"{len(groepen['onvolledig'])} met een lege kolom.")

    if not args.apply:
        print("\nDroogloop. Draai met --apply om dit weg te schrijven.")
        con.close()
        return 0

    kopie = DB_PATH.replace(".db", f".backup-{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
    shutil.copy2(DB_PATH, kopie)
    print(f"\nBack-up: {os.path.basename(kopie)}")

    for r in groepen["gekopieerd"]:
        con.execute("""UPDATE historical_metrics SET deelnemers_actief = NULL,
                       deelnemers_slapers = NULL, deelnemers_pensioengerechtigd = NULL
                       WHERE rowid = ?""", (r["rid"],))
    for r in groepen["totaal_mis"]:
        con.execute("UPDATE historical_metrics SET deelnemers_totaal = NULL WHERE rowid = ?",
                    (r["rid"],))
    con.commit()
    print(f"{len(groepen['gekopieerd'])} uitsplitsingen en "
          f"{len(groepen['totaal_mis'])} totalen leeggemaakt.")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
