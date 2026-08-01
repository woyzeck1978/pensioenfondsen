"""Voeg de pensioenfondsen toe die wel bij DNB rapporteren maar niet in funds staan.

De DNB-feed telt 188 rapporteurs, de fondsentabel koppelde er 173. De vijftien
die overbleven stonden in `load_dnb_quarterly.MANUAL_MAP` op None met als
toelichting "not in DB" — een cirkelredenering: ze ontbraken juist omdat niemand
ze had toegevoegd.

Zo viel het schoonmaakfonds uit de dataset. Bpf Schoonmaak- en
Glazenwassersbedrijf heeft 44 kwartalen DNB-historie en 6,8 miljard vermogen;
het is per 1 januari 2026 ingevaren ("Het fondsvermogen is verdeeld", eigen
nieuwsbericht). De rij verdween bij een ontdubbeling vóór de eerste DB-commit,
maar zijn nieuws, documenten en jaarreeks bleven achter op fonds-id 61. Daarom
krijgen Schoonmaak en Flexsecurity hun oorspronkelijke id terug: dan haakt alles
wat naar hen verwees vanzelf weer aan.

Niet elke ongekoppelde rapporteur is een ontbrekend fonds. Vier ervan zijn de
voorganger van een kring die al in de tabel staat, en of dat zo is, is aan de
perioden te zien. CRH rapporteert tot en met 2024Q4 en Pensioenkring CRH begint
in 2025Q1: dat sluit naadloos aan, dus dezelfde stichting onder een nieuwe vlag.
Grolsche loopt door tot 2025Q3 terwijl Kring Grolsch al in 2024Q4 begint — vier
kwartalen overlap, dus twee verschillende dingen. Bij overlap koppelen we niet,
want dan tel je hetzelfde vermogen twee keer.

  python3 scripts/db_management/herstel_ontbrekende_fondsen.py
  python3 scripts/db_management/herstel_ontbrekende_fondsen.py --apply
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import shutil
import sqlite3
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")
FEED_PATH = os.path.join(BASE_DIR, "data", "processed", "dnb_per_fund_quarterly_raw.json")

# DNB rapporteert bedragen in duizenden euro.
NAAR_MRD = 1e6
VERMOGEN = ("Beleggingen voor risico fonds", "Beleggingen voor risico deelnemer")

# Fondsen die aan de DNB-feed zijn te ontlenen maar in funds ontbraken.
# vast_id: het id waarop hun verweesde rijen al wachten.
TOEVOEGEN = [
    # (DNB-naam, naam in funds, categorie, website, vast_id)
    ("Schoonmaak- en Glazenwassersbedrijf", "Schoonmaak- en Glazenwassersbedrijf",
     "Bedrijfstakpensioenfonds", "https://www.pensioenschoonmaak.nl/", 61),
    ("Flexsecurity", "Flexsecurity (Randstad)",
     "Ondernemingspensioenfonds", "https://www.flexsecuritypensioen.nl/", 95),
    ("Tandartsen en Tandarts-Specialisten", "Tandartsen en Tandarts-Specialisten",
     "Beroepspensioenfonds", None, None),
    ("Honeywell", "Honeywell", "Ondernemingspensioenfonds", None, None),
    ("Nedlloyd", "Nedlloyd", "Ondernemingspensioenfonds", None, None),
    ("Pensura", "Pensura", "Ondernemingspensioenfonds", None, None),
    ("YARA Nederland", "YARA Nederland", "Ondernemingspensioenfonds", None, None),
    ("Coram", "Coram", "Ondernemingspensioenfonds", None, None),
    ("Calpam", "Calpam", "Ondernemingspensioenfonds", None, None),
]

# Rapporteurs die de voorganger zijn van een rij die al bestaat. Alleen koppelen
# als de perioden elkaar niet overlappen; dat toetst het script zelf.
VOORGANGERS = {"CRH": 187, "Wolters Kluwer Nederland": 194, "Staples": 177}


def feed_per_naam() -> dict[str, dict]:
    with open(FEED_PATH) as f:
        rijen = json.load(f)["data"]
    uit: dict[str, dict] = {}
    for naam, metriek, tijd, waarde in rijen:
        d = uit.setdefault(naam, {"perioden": set(), "vermogen": {}, "dgr": None})
        d["perioden"].add(tijd)
        if waarde is None:
            continue
        if metriek in VERMOGEN:
            vorig = d["vermogen"].get(metriek)
            if not vorig or tijd > vorig[0]:
                d["vermogen"][metriek] = (tijd, waarde)
        elif metriek == "Beleidsdekkingsgraad" and (not d["dgr"] or tijd > d["dgr"][0]):
            d["dgr"] = (tijd, waarde)
    return uit


def kwartaal(ms: int) -> str:
    d = datetime.date.fromtimestamp(ms / 1000)
    return f"{d.year}Q{(d.month - 1) // 3 + 1}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="voeg de fondsen toe")
    args = ap.parse_args()

    feed = feed_per_naam()
    laatste_periode = max(t for d in feed.values() for t in d["perioden"])
    con = sqlite3.connect(DB_PATH)
    bezet = {r[0] for r in con.execute("SELECT id FROM funds")}
    volgend = max(bezet) + 1

    plan = []
    for dnb_naam, naam, categorie, site, vast in TOEVOEGEN:
        d = feed.get(dnb_naam)
        if not d:
            print(f"  {dnb_naam}: komt niet in de feed voor — overgeslagen")
            continue
        if vast is not None and vast in bezet:
            print(f"  {naam}: id {vast} is inmiddels bezet — overgeslagen")
            continue
        fid = vast if vast is not None else volgend
        if vast is None:
            volgend += 1
        eind = max(d["perioden"])
        aum = sum(v for _, v in d["vermogen"].values()) / NAAR_MRD or None
        plan.append({
            "id": fid, "dnb": dnb_naam, "name": naam, "category": categorie,
            "website": site, "aum": round(aum, 3) if aum else None,
            "dgr": d["dgr"][1] if d["dgr"] else None,
            "kwartalen": len(d["perioden"]), "eind": kwartaal(eind),
            "loopt_nog": eind == laatste_periode,
        })

    print(f"{len(plan)} fondsen toevoegen (DNB-feed loopt tot {kwartaal(laatste_periode)}):\n")
    for p in plan:
        wat = "rapporteert nog" if p["loopt_nog"] else f"laatste rapportage {p['eind']}"
        print(f"  id {p['id']:>4}  {p['name'][:36]:<38} {str(p['aum'] or '?'):>7} mrd  "
              f"beleidsdg {p['dgr'] or '?':>6}  {p['kwartalen']:>3} kw, {wat}")

    print("\nVoorgangers van een bestaande rij — koppelen als de perioden aansluiten:\n")
    koppel = {}
    for dnb_naam, fid in VOORGANGERS.items():
        d = feed.get(dnb_naam)
        if not d:
            print(f"  {dnb_naam:<28} niet in de feed")
            continue
        eind_los = max(d["perioden"])
        r = con.execute("""SELECT f.name, MIN(dq.year * 10 + dq.quarter), COUNT(*)
                           FROM funds f LEFT JOIN dnb_quarterly_metrics dq ON dq.fund_id = f.id
                           WHERE f.id = ? GROUP BY f.id""", (fid,)).fetchone()
        if not r:
            print(f"  {dnb_naam:<28} fonds {fid} bestaat niet")
            continue
        start_rij = r[1]
        eind_getal = int(kwartaal(eind_los).replace("Q", ""))
        if start_rij and start_rij <= eind_getal:
            print(f"  {dnb_naam:<28} OVERLAP met {r[0][:26]} (die begint in "
                  f"{start_rij // 10}Q{start_rij % 10}) — niet koppelen")
            continue
        koppel[dnb_naam] = fid
        print(f"  {dnb_naam:<28} tot {kwartaal(eind_los)} -> {fid} {r[0][:30]} "
              f"(begint {start_rij // 10}Q{start_rij % 10 if start_rij else '?'})"
              if start_rij else
              f"  {dnb_naam:<28} tot {kwartaal(eind_los)} -> {fid} {r[0][:30]} (nog geen DNB-rijen)")

    if not args.apply:
        print("\nDroogloop. Draai met --apply om de fondsen toe te voegen.")
        print("Daarna load_dnb_quarterly.py draaien om hun kwartaalreeks te laden.")
        con.close()
        return 0

    kopie = DB_PATH.replace(".db", f".backup-{datetime.datetime.now():%Y%m%d_%H%M%S}.db")
    shutil.copy2(DB_PATH, kopie)
    print(f"\nBack-up: {os.path.basename(kopie)}")

    for p in plan:
        # Een fonds dat niet meer rapporteert is gesloten; Schoonmaak meldde zelf
        # dat het vermogen is verdeeld, wat invaren is.
        status = ("Open" if p["loopt_nog"]
                  else "Ingevaren" if p["id"] == 61 else "Gesloten")
        con.execute("""INSERT INTO funds
            (id, name, category, website, aum_euro_bn, beleidsdekkingsgraad_pct,
             status, is_pensioenfonds, data_source, description)
            VALUES (?,?,?,?,?,?,?,1,'DNB kwartaalstatistiek',?)""",
            (p["id"], p["name"], p["category"], p["website"], p["aum"], p["dgr"], status,
             f"Toegevoegd uit de DNB-kwartaalfeed ({p['kwartalen']} kwartalen, "
             f"laatste {p['eind']}); overige velden nog niet uit het jaarverslag gevuld."))
    con.commit()
    print(f"{len(plan)} fondsen toegevoegd.")
    if koppel:
        print("Zet deze koppelingen in MANUAL_MAP van load_dnb_quarterly.py:")
        for n, f in koppel.items():
            print(f'    "{n}": {f},')
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
