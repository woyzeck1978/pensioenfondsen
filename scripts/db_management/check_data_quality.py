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


def dnb_koppeling(con):
    """Rapporteurs in de verse DNB-feed die aan geen enkel fonds gekoppeld zijn.

    Hernoemen of samenvoegen van fondsen is veilig binnen de database, maar
    breekt de brug naar externe bronnen die op naam matchen. Dat viel in juli
    2026 pas op toen BPFBouw met 69,5 mld stilletjes uit de DNB-reeks verdween.
    Alleen rapporteurs met data in de nieuwste periode tellen mee: wie al jaren
    niets meer aanlevert is opgeheven, niet losgeraakt.
    """
    import json
    pad = os.path.join(BASE_DIR, "data", "processed", "dnb_per_fund_quarterly_raw.json")
    if not os.path.exists(pad):
        return []
    with open(pad) as f:
        feed = json.load(f)
    rijen = feed.get("data", [])
    if not rijen:
        return []
    laatste = max(r[2] for r in rijen)
    actief = {r[0] for r in rijen if r[2] == laatste}

    # De loader matcht in drie stappen (handmatig, exact, deelstring). Die logica
    # hier nabouwen zou vals alarm geven, dus we gebruiken zijn eigen functie.
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from load_dnb_quarterly import build_name_map, MANUAL_MAP
    except Exception as e:
        return [f"kon load_dnb_quarterly niet importeren om de koppeling te toetsen: {e}"]

    db_names = con.execute(f"SELECT id, name FROM funds WHERE {LEVEND}").fetchall()
    mapping = build_name_map([(r["id"], r["name"]) for r in db_names], sorted(actief))
    # Op None gezette namen zijn bewust uitgesloten — DNB heeft soms twee
    # rapporteurs waar wij één fonds voeren; die mogen niet dubbel tellen.
    bewust = {k for k, v in MANUAL_MAP.items() if v is None}
    return [f"{n} — levert nog aan maar wordt door de loader overgeslagen"
            for n in sorted(actief) if n not in mapping and n not in bewust]


def schrijfwijzen(con):
    """Waarden die alleen in hoofdletters of spaties verschillen.

    'Achmea', 'ACHMEA' en 'Achmea Pensioenservices N.V.' stonden als vier
    verschillende uitvoerders in de database en splitsten daarmee elke
    concentratiegrafiek op. Zulke varianten zijn met het oog nauwelijks te
    zien in een lange lijst, maar wel automatisch te vinden.
    """
    uit = []
    for kolom in ("uitvoerder", "fiduciair_beheerder", "category"):
        groepen = defaultdict(set)
        try:
            rijen = con.execute(
                f"SELECT {kolom} FROM funds WHERE {LEVEND} AND COALESCE({kolom},'') <> ''")
        except sqlite3.Error:
            continue
        for (v,) in rijen:
            groepen["".join(ch for ch in v.lower() if ch.isalnum())].add(v)
        for _, varianten in groepen.items():
            if len(varianten) > 1:
                uit.append(f"{kolom}: " + " | ".join(sorted(varianten)))
    return uit


def regiogewichten(con):
    """Geografische gewichten die niet kunnen kloppen.

    De regiotabel bleek herschaald: waar een jaarverslag alleen 'Emerging
    Markets: 7,0' meldde, stond er 100%. Bij ABP en PFZW leidde dat tot
    'Nederland 100%', wat voor een wereldwijd beleggend fonds onmogelijk is.
    Twee signalen verraden zulke vervuiling: één regio die precies honderd
    procent claimt, en fondsen waarvan de regio's samen boven de honderd
    uitkomen.
    """
    uit = []
    try:
        rijen = con.execute("""
            SELECT e.fund_id, f.name, COUNT(*) n, SUM(e.weight_pct) som,
                   MIN(e.region) regio, MAX(e.weight_pct) maxw
            FROM equity_strategies e JOIN funds f ON f.id = e.fund_id
            GROUP BY e.fund_id""").fetchall()
    except sqlite3.Error:
        return []
    for r in rijen:
        if r["n"] == 1 and r["maxw"] >= 100:
            uit.append(f"{r['name'][:34]:36s} één regio ({r['regio']}) op {r['maxw']:.0f}%")
        elif r["som"] and r["som"] > 105:
            uit.append(f"{r['name'][:34]:36s} regio's tellen op tot {r['som']:.0f}%")
    return uit


def deelnemers_gedupliceerd_binnen_fonds(con):
    """Twee deelnemerskolommen met exact hetzelfde getal.

    Dit is het patroon dat bij Rockwool, Vopak en Avery Dennison bleek te zitten:
    de extractie vindt één getal en schrijft dat in twee velden. Dat een fonds
    toevallig precies evenveel slapers als gepensioneerden heeft komt voor, maar
    boven de honderd wordt het onwaarschijnlijk genoeg om naar te kijken.
    """
    uit = []
    for r in con.execute(f"""
        SELECT id, name, deelnemers_actief a, deelnemers_slapers s, deelnemers_gepensioneerd g
        FROM funds WHERE {LEVEND}
    """):
        paren = (("actief", "slapers", r["a"], r["s"]),
                 ("actief", "gepensioneerd", r["a"], r["g"]),
                 ("slapers", "gepensioneerd", r["s"], r["g"]))
        for k1, k2, v1, v2 in paren:
            if v1 is not None and v1 == v2 and v1 > 100:
                uit.append(f"{r['name'][:40]:42s} {k1} en {k2} allebei {v1:,}")
    return uit


def vermogen_per_deelnemer(con):
    """Een pensioenvermogen dat niet in verhouding staat tot het deelnemersaantal.

    Exxonmobil stond op 107,9 miljoen deelnemers bij €2,9 mld — €27 per hoofd.
    De ondergrens is bewust ruim: bij een premiepensioeninstelling of een
    uitzendfonds met jonge deelnemers is een paar duizend euro per hoofd normaal,
    dus die vallen er niet in. Onder de duizend euro kán het gewoon niet.
    """
    uit = []
    for r in con.execute(f"""
        SELECT name, deelnemers_totaal t, aum_euro_bn aum FROM funds
        WHERE {LEVEND} AND deelnemers_totaal > 0 AND aum_euro_bn > 0
    """):
        per = r["aum"] * 1e9 / r["t"]
        if per < 1000:
            uit.append(f"{r['name'][:40]:42s} {r['t']:,} deelnemers bij "
                       f"EUR {r['aum']:.2f} mld = EUR {per:,.0f} per deelnemer")
        elif per > 3_000_000:
            uit.append(f"{r['name'][:40]:42s} {r['t']:,} deelnemers bij "
                       f"EUR {r['aum']:.2f} mld = EUR {per:,.0f} per deelnemer")
    return uit


CONTROLES = [
    ("Deelnemers tellen niet op tot het totaal", deelnemers_inconsistent),
    ("Hetzelfde getal in twee deelnemerskolommen", deelnemers_gedupliceerd_binnen_fonds),
    ("Vermogen per deelnemer buiten elke verhouding", vermogen_per_deelnemer),
    ("Zelfde deelnemersuitsplitsing bij meerdere fondsen",
     lambda c: gedeelde_waarden(c, ["deelnemers_actief", "deelnemers_slapers", "deelnemers_gepensioneerd"], "")),
    ("Zelfde deelnemerstotaal bij meerdere fondsen",
     lambda c: gedeelde_waarden(c, ["deelnemers_totaal"], "", drempel=1000)),
    ("Meerdere fondsen op dezelfde website", dubbele_fondsen),
    ("APF-moeder telt dubbel met zijn kringen", apf_dubbeltelling),
    ("DNB-rapporteurs zonder koppeling aan een fonds", dnb_koppeling),
    ("Dezelfde partij onder meerdere schrijfwijzen", schrijfwijzen),
    ("Geografische gewichten die niet kunnen kloppen", regiogewichten),
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
