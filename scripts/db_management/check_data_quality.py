#!/usr/bin/env python3
"""Controleert de database op de datafouten die de scraper stilletjes maakt.

Aanleiding: de deelnemerskolommen bleken bij 30 van de 146 fondsen intern
inconsistent doordat cijfers van het ene fonds naar het andere waren
overgeschreven. Zulke fouten vallen niet op in het dashboard — een fonds toont
gewoon een getal — dus ze moeten actief opgespoord worden.

Gebruik:
    python3 scripts/db_management/check_data_quality.py            # rapport
    python3 scripts/db_management/check_data_quality.py --strict   # exit 1 bij fouten

Het script wijzigt niets; het rapporteert alleen.
"""

import argparse
import os
import sqlite3
import sys
from collections import defaultdict

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

# Fondsen die als duplicaat zijn samengevoegd horen buiten elke controle te vallen.
LEVEND = "COALESCE(status,'') NOT LIKE 'Duplicaat%'"


def verbind(pad):
    con = sqlite3.connect(pad)
    con.row_factory = sqlite3.Row
    return con


def deelnemers_inconsistent(con):
    """Uitsplitsing die niet optelt tot het totaal — dan klopt minstens een van beide niet."""
    rijen = con.execute(f"""
        SELECT id, name, deelnemers_actief a, deelnemers_slapers s,
               deelnemers_gepensioneerd g, deelnemers_totaal t
        FROM funds
        WHERE {LEVEND} AND deelnemers_totaal > 0 AND deelnemers_actief IS NOT NULL
    """).fetchall()
    uit = []
    for r in rijen:
        som = (r["a"] or 0) + (r["s"] or 0) + (r["g"] or 0)
        if som > 0 and abs(r["t"] - som) > 0.01 * r["t"]:
            uit.append(f"{r['name'][:40]:42s} {r['a']}+{r['s']}+{r['g']} = {som}, maar totaal = {r['t']}")
    return uit


def gedeelde_waarden(con, kolommen, label, drempel=100):
    """Twee fondsen met exact dezelfde cijfers betekent overschrijven, geen toeval."""
    velden = ", ".join(kolommen)
    groepen = defaultdict(list)
    for r in con.execute(f"SELECT id, name, {velden} FROM funds WHERE {LEVEND}"):
        sleutel = tuple(r[k] for k in kolommen)
        if any(v is None for v in sleutel):
            continue
        if all(isinstance(v, (int, float)) and v < drempel for v in sleutel):
            continue  # kleine getallen kunnen legitiem samenvallen
        groepen[sleutel].append(r["name"])
    return [f"{label} {sleutel}: " + " | ".join(n[:28] for n in namen)
            for sleutel, namen in groepen.items() if len(namen) > 1]


def dubbele_fondsen(con):
    """Zelfde website betekent doorgaans dezelfde stichting, twee keer ingelezen.

    APF-kringen delen legitiem de site van hun moederfonds, dus die blijven
    buiten beschouwing — anders verdrinken de echte duplicaten in de ruis.
    """
    groepen = defaultdict(list)
    for r in con.execute(f"""SELECT id, name, website FROM funds
                             WHERE {LEVEND} AND website IS NOT NULL AND website <> ''
                               AND name NOT LIKE 'Kring %' AND name NOT LIKE 'Pensioenkring %'
                               AND COALESCE(category,'') <> 'APF'"""):
        # alleen het domein vergelijken: dezelfde stichting krijgt soms een
        # diepe link naar de jaarverslagpagina en soms de homepage
        site = r["website"].lower().split("//")[-1].split("/")[0].removeprefix("www.")
        groepen[site].append(f"{r['name'][:30]} (id {r['id']})")
    return [f"{site}: " + " | ".join(namen) for site, namen in groepen.items() if len(namen) > 1]


def apf_dubbeltelling(con):
    """Een APF-moeder met eigen vermogen naast dat van zijn kringen telt dubbel."""
    patronen = {r["fund_id"]: r["kring_patroon"] for r in
                con.execute("SELECT fund_id, kring_patroon FROM apf_profiel")} \
        if con.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='apf_profiel'").fetchone()[0] \
        else {}
    uit = []
    for fid, patroon in patronen.items():
        moeder = con.execute("SELECT name, aum_euro_bn FROM funds WHERE id=?", (fid,)).fetchone()
        if not moeder or not moeder["aum_euro_bn"]:
            continue
        som = 0.0
        for deel in patroon.split("|"):
            r = con.execute(f"SELECT COALESCE(SUM(aum_euro_bn),0) s FROM funds WHERE {LEVEND} AND name LIKE ?",
                            (f"%{deel}%",)).fetchone()
            som += r["s"]
        if som > 0:
            uit.append(f"{moeder['name'][:34]:36s} moeder {moeder['aum_euro_bn']:.2f} mld naast "
                       f"{som:.2f} mld aan kringen")
    return uit


CONTROLES = [
    ("Deelnemers tellen niet op tot het totaal", deelnemers_inconsistent),
    ("Zelfde deelnemersuitsplitsing bij meerdere fondsen",
     lambda c: gedeelde_waarden(c, ["deelnemers_actief", "deelnemers_slapers", "deelnemers_gepensioneerd"], "")),
    ("Zelfde deelnemerstotaal bij meerdere fondsen",
     lambda c: gedeelde_waarden(c, ["deelnemers_totaal"], "", drempel=1000)),
    ("Meerdere fondsen op dezelfde website", dubbele_fondsen),
    ("APF-moeder telt dubbel met zijn kringen", apf_dubbeltelling),
]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", default=DB_PATH)
    p.add_argument("--strict", action="store_true", help="exit-code 1 zodra er iets gevonden wordt")
    args = p.parse_args()

    con = verbind(args.db)
    totaal = 0
    for kop, fn in CONTROLES:
        try:
            bevindingen = fn(con)
        except sqlite3.Error as e:
            print(f"\n{kop}\n  overgeslagen: {e}")
            continue
        totaal += len(bevindingen)
        merk = "OK" if not bevindingen else f"{len(bevindingen)} gevonden"
        print(f"\n{kop} — {merk}")
        for regel in sorted(bevindingen)[:20]:
            print(f"  {regel}")
        if len(bevindingen) > 20:
            print(f"  … en nog {len(bevindingen) - 20}")
    con.close()

    print(f"\nTotaal: {totaal} bevinding(en).")
    if args.strict and totaal:
        sys.exit(1)


if __name__ == "__main__":
    main()
