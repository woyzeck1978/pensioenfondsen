"""Voeg funds.is_pensioenfonds toe: welke rijen zijn écht een pensioenfonds?

De tabel bevat naast fondsen ook verzekeraars, premiepensioeninstellingen en
uitvoerders. Die telden mee in het sectortotaal, waardoor het dashboard
1.698,6 miljard toonde in plaats van 1.613,6 — Nationale-Nederlanden (36
miljard) en Allianz (15 miljard) werden als pensioenfonds meegeteld.

Het dashboard leidde dat sinds kort af uit categorie plus een namenlijst, maar
dan zit de afbakening in presentatiecode: wie de database rechtstreeks bevraagt
of de Excel-export gebruikt, telt ze opnieuw mee. Daarom nu een expliciete
kolom.

De regel:
  0  categorie 'Verzekeraar' of 'PPI'   — pensioenuitvoerders, geen fondsen
  0  APG, A.S. Watson Nederland          — uitvoerder resp. registerrij; die
                                          staan als 'Bedrijf' geboekt, net als
                                          de echte ondernemingsfondsen, en zijn
                                          alleen op naam te onderscheiden
  1  al het overige

  python3 scripts/db_management/add_is_pensioenfonds.py
  python3 scripts/db_management/add_is_pensioenfonds.py --apply
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

GEEN_FONDS_CATEGORIE = ("Verzekeraar", "PPI")
GEEN_FONDS_NAAM = ("APG", "A.S. Watson Nederland")

# Nederlandse regelingen die worden uitgevoerd door een buitenlandse instelling.
# Dat zijn geen Nederlandse pensioenfondsen: het prudentieel toezicht ligt in
# Belgie en ze komen niet voor in DNB's kwartaalstatistiek. Waar een gewoon fonds
# vijfhonderd DNB-rijen heeft, hebben deze drie er nul. Hun 5,84 miljard telde
# wel mee in het sectortotaal.
GEEN_FONDS_OFP = {
    44: "Nederlandse sectie binnen OFP BP Pensioenfonds (Belgie); geen DNB-toezicht",
    94: "Nederlandse regeling binnen ExxonMobil OFP, Machelen (Belgie); geen DNB-toezicht",
    109: "Nederlandse sectie binnen de Belgische OFP van J&J; jaarstukken bij de NBB gedeponeerd",
}
REDEN_CATEGORIE = "verzekeraar of PPI, geen pensioenfonds"
REDEN_NAAM = "uitvoerder of registerrij, geen pensioenfonds"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="voeg de kolom toe en vul hem")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    bestaat = any(r[1] == "is_pensioenfonds"
                  for r in con.execute("PRAGMA table_info(funds)"))

    vragen = ",".join("?" * len(GEEN_FONDS_CATEGORIE))
    namen = ",".join("?" * len(GEEN_FONDS_NAAM))
    ofp = ",".join(str(i) for i in GEEN_FONDS_OFP)
    geen = con.execute(
        f"""SELECT id, name, category, COALESCE(aum_euro_bn, 0) FROM funds
            WHERE COALESCE(category,'') IN ({vragen}) OR name IN ({namen})
               OR id IN ({ofp})
            ORDER BY aum_euro_bn DESC""",
        (*GEEN_FONDS_CATEGORIE, *GEEN_FONDS_NAAM)).fetchall()

    totaal = con.execute("SELECT COUNT(*), ROUND(SUM(COALESCE(aum_euro_bn,0)),1) FROM funds").fetchone()
    print(f"{totaal[0]} rijen in funds, samen {totaal[1]:,.1f} miljard\n".replace(",", "."))
    print(f"Als géén pensioenfonds gemarkeerd ({len(geen)}):")
    for _, naam, cat, aum in geen:
        print(f"  {aum:>6.1f} mrd  {naam[:38]:<39} {cat or '(geen categorie)'}")
    weg = sum(r[3] for r in geen)
    print(f"\n  samen {weg:.1f} miljard — sectortotaal wordt {totaal[1] - weg:.1f}")
    print(f"  kolom is_pensioenfonds bestaat al: {'ja' if bestaat else 'nee'}")

    if not args.apply:
        print("\nDroogloop. Draai met --apply om de kolom toe te voegen en te vullen.")
        con.close()
        return 0

    if not bestaat:
        con.execute("ALTER TABLE funds ADD COLUMN is_pensioenfonds INTEGER DEFAULT 1")
    if not any(r[1] == "afbakening_reden" for r in con.execute("PRAGMA table_info(funds)")):
        # De reden hoort bij de uitkomst. Stond die alleen in dit script, dan is
        # bij een rij met is_pensioenfonds=0 niet te zien waarom.
        con.execute("ALTER TABLE funds ADD COLUMN afbakening_reden TEXT")
    con.execute("UPDATE funds SET is_pensioenfonds = 1, afbakening_reden = NULL")
    con.execute(
        f"""UPDATE funds SET is_pensioenfonds = 0, afbakening_reden = ?
            WHERE COALESCE(category,'') IN ({vragen})""",
        (REDEN_CATEGORIE, *GEEN_FONDS_CATEGORIE))
    con.execute(
        f"""UPDATE funds SET is_pensioenfonds = 0, afbakening_reden = ?
            WHERE name IN ({namen})""", (REDEN_NAAM, *GEEN_FONDS_NAAM))
    for fid, reden in GEEN_FONDS_OFP.items():
        con.execute("UPDATE funds SET is_pensioenfonds = 0, afbakening_reden = ? WHERE id = ?",
                    (reden, fid))
    con.commit()
    n0, n1 = con.execute(
        "SELECT SUM(is_pensioenfonds = 0), SUM(is_pensioenfonds = 1) FROM funds").fetchone()
    print(f"\nKolom gevuld: {n1} pensioenfondsen, {n0} andere rijen.")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
