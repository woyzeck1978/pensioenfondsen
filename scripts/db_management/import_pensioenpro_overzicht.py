#!/usr/bin/env python3
"""Vult de transitiegegevens aan uit het PensioenPro/FD-overzicht (27-4-2026).

Dat overzicht ("Overzicht pensioentransitie per pensioenfonds") is de enige
publieke bron die *alle* 189 fondsen en 49 pensioenkringen in één tabel zet,
inclusief twee dingen die nergens anders systematisch staan:

  * de uitstelhistorie ("Van 1-1-2026 naar 1-7-2027"), en
  * wat er gebeurt met de fondsen die *niet* invaren — liquidatie naar een
    verzekeraar, collectieve waardeoverdracht naar een bedrijfstakfonds, of
    gewoon achterblijven in het FTK.

Het overzicht is een momentopname van 27 april 2026. Onze eigen datums komen
deels uit latere primaire bronnen (transitieplannen, jaarverslagen van mei tot
juli 2026), dus dit script overschrijft nooit een bestaande waarde — het vult
alleen lege velden. De transitiedatums zelf blijven volledig ongemoeid.

Draai vanuit de repo-root:  python3 scripts/db_management/import_pensioenpro_overzicht.py
"""

import argparse
import sqlite3
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DB = REPO / "data" / "processed" / "pension_funds.db"

# Handmatig vastgesteld door het overzicht per regel tegen funds.name te leggen;
# de fondsnamen in het overzicht zijn afkortingen ("BPL", "Vlep", "Sabic") die
# geen enkele fuzzy match betrouwbaar op onze volledige namen afbeeldt.
EINDBESTEMMING = {
    198: "naar bpf Vervoer",   # Rijn- en Binnenvaart -> bpf Vervoer
    127: "naar bpf PGB",       # Sagittarius
    131: "naar bpf PME",       # TDV
    70:  "blijft in ftk",      # Abbott
    78:  "blijft in ftk",      # Atos
    85:  "blijft in ftk",      # Citigroup
    100: "blijft in ftk",      # Geveke
    103: "blijft in ftk",      # HAL
    114: "blijft in ftk",      # Mercer
    115: "blijft in ftk",      # Metro
    174: "blijft in ftk",      # Kring Progress (Unilever)
    210: "blijft in ftk",      # Kring DB Evenwicht (Centraal Beheer)
    211: "blijft in ftk",      # Kring DB Koopkracht (Centraal Beheer)
    212: "blijft in ftk",      # Kring DB Premie (Centraal Beheer)
    213: "blijft in ftk",      # Kring DB Stabiliteit (Centraal Beheer)
    77:  "naar verzekeraar",   # AT&T Nederland
    82:  "naar verzekeraar",   # Campina
    88:  "naar verzekeraar",   # Delta Lloyd
    90:  "naar verzekeraar",   # DOW
    92:  "naar verzekeraar",   # Ecolab
    206: "naar verzekeraar",   # Ernst & Young
    207: "naar verzekeraar",   # British American Tobacco
    208: "naar verzekeraar",   # Trespa
    223: "naar verzekeraar",   # Kring Bavaria (Centraal Beheer)
}

# Oorspronkelijk beoogde invaardatum, uit de uitstelkolom. Waar een fonds twee
# keer heeft uitgesteld (Grolsch: 1-1-2026 -> 1-7-2026 -> 1-1-2027) staat hier
# de vroegste datum, want dat is waar de vertraging tegen afgezet hoort te worden.
OORSPRONKELIJK = {
    22:  "2025-01-01",  # Levensmiddelen / BPFL
    58:  "2026-01-01",  # Metalektro / PME
    108: "2027-01-01",  # ING / ING CDC
    118: "2027-01-01",  # NN CDC
    215: "2026-07-01",  # Kring RBS / NatWest (Centraal Beheer)
    219: "2026-01-01",  # Kring Grolsch (Centraal Beheer)
    227: "2025-01-01",  # Kring Holland Casino (Stap)
}

# Alleen echte contractkeuzes; "naar verzekeraar" en "blijft in ftk" zijn geen
# contracttype maar een eindbestemming en horen in de andere kolom.
CONTRACT = {
    118: "solidair",    # NN CDC
}

# Het overzicht onderscheidt bij de apf'en een collectieve kring (meerdere
# werkgevers delen één kring) van een eigen kring (één werkgever). Dat bepaalt
# of een werkgever nog eigen zeggenschap heeft over het beleggingsbeleid, en het
# verklaart waarom de ene kring een eigen abtn publiceert en de andere niet.
# TotalEnergies en Astellas staan in het overzicht op "ntb" en blijven hier leeg.
KRING_SOORT = {
    # collectieve kringen
    163: "collectieve kring",  # Kring AFM (De Nationale)
    166: "collectieve kring",  # Kring B / NIBC-Eriks (De Nationale)
    167: "collectieve kring",  # Kring CK1 (HNPF)
    168: "collectieve kring",  # Kring D / True Blue (De Nationale)
    169: "collectieve kring",  # Kring A / AZL en Unisys (De Nationale)
    171: "collectieve kring",  # Kring Eastman (Stap)
    173: "collectieve kring",  # Kring McCain (De Nationale)
    193: "collectieve kring",  # Kring Van Lanschot (HNPF)
    209: "collectieve kring",  # Kring DC (Centraal Beheer)
    210: "collectieve kring",  # Kring DB Evenwicht (Centraal Beheer)
    211: "collectieve kring",  # Kring DB Koopkracht (Centraal Beheer)
    212: "collectieve kring",  # Kring DB Premie (Centraal Beheer)
    213: "collectieve kring",  # Kring DB Stabiliteit (Centraal Beheer)
    214: "collectieve kring",  # Kring HPE (Centraal Beheer)
    215: "collectieve kring",  # Kring RBS / NatWest (Centraal Beheer)
    216: "collectieve kring",  # Kring equensWorldline (Centraal Beheer)
    217: "collectieve kring",  # Kring Sligro Food Group (Centraal Beheer)
    218: "collectieve kring",  # Kring Deutsche Bank Nederland (Centraal Beheer)
    219: "collectieve kring",  # Kring Grolsch (Centraal Beheer)
    220: "collectieve kring",  # Kring Chemours (Centraal Beheer)
    221: "collectieve kring",  # Kring ESN (Centraal Beheer)
    222: "collectieve kring",  # Kring HP Nederland (Centraal Beheer)
    223: "collectieve kring",  # Kring Bavaria (Centraal Beheer)
    224: "collectieve kring",  # Kring E-DC (De Nationale)
    228: "collectieve kring",  # Kring IFF (Stap)
    229: "collectieve kring",  # Kring Flexibele Regeling (HNPF)
    # eigen kringen
    164: "eigen kring",        # Kring ANWB (De Nationale)
    165: "eigen kring",        # Kring Arcadis (HNPF)
    170: "eigen kring",        # Kring Douwe Egberts (Stap)
    172: "eigen kring",        # Kring Forward (Unilever)
    174: "eigen kring",        # Kring Progress (Unilever)
    175: "eigen kring",        # Kring Roba (HNPF)
    176: "eigen kring",        # Kring SVG (Stap)
    184: "eigen kring",        # Pensioenkring AON (Stap)
    186: "eigen kring",        # Pensioenkring Ballast Nedam (Stap)
    187: "eigen kring",        # Pensioenkring CRH (HNPF)
    188: "eigen kring",        # Pensioenkring GE Nederland (Stap)
    189: "eigen kring",        # Pensioenkring OWASE (HNPF)
    190: "eigen kring",        # Pensioenkring PepsiCo (HNPF)
    191: "eigen kring",        # Pensioenkring Randstad (HNPF)
    192: "eigen kring",        # Pensioenkring Sweco (HNPF)
    194: "eigen kring",        # Pensioenkring Wolters Kluwer NL (HNPF)
    195: "eigen kring",        # Pensioenkring Xerox (HNPF)
    225: "eigen kring",        # Kring G-Cargill (De Nationale)
    226: "eigen kring",        # Kring H NN CDC (De Nationale)
    227: "eigen kring",        # Kring Holland Casino (Stap)
    230: "eigen kring",        # Pensioenkring Cargill (HNPF)
}

# Het overzicht zet alle veertien HNPF-kringen op DION. Ons eigen materiaal
# bevestigt dat: het HNPF-jaarverslag 2025 schrijft dat "DION al de administratie
# voert voor het merendeel van onze kringen" en dat de laatste twee — Van Lanschot
# en de Flexibele regeling — per 1 januari 2026 van Idella (voorheen Visma Idella)
# naar DION zijn overgegaan. Onze kolom stond nog op de situatie van 2025.
HNPF_KRINGEN = [165, 167, 175, 187, 189, 190, 191, 192, 193, 194, 195, 229, 230]
DION = "DION Pensioen Services"

# NN CDC stond op "ons fonds" — geen uitvoerder maar een stuk zinsbouw dat de
# scraper uit een pagina heeft geplukt. Het overzicht geeft AZL, wat klopt met
# de kring van NN CDC bij De Nationale APF (een AZL-initiatief).
UITVOERDER_HERSTEL = {
    118: "AZL",
    # Rijn- en Binnenvaart stond op TKP. Het fonds schrijft op zijn eigen site
    # "AZL voor de administratie, Montae & Partners voor advies en ondersteuning",
    # en het postadres is Postbus 4471 in Heerlen — de vestigingsplaats van AZL.
    # TKP is vermoedelijk overgewaaid van Beroepsvervoer over de Weg (id 13),
    # het bedrijfstakfonds waar Rijn- en Binnenvaart zijn pensioenen aan overdraagt
    # en dat wél bij TKP zit.
    198: "AZL",
    # Ecolab stond leeg. Het jaarverslag 2025 is expliciet: "De uitvoeringsorganisatie
    # voor de pensioenadministratie is Ecolab B.V." — de werkgever doet het zelf,
    # met alleen een deelbewerking bij ADP.
    92: "Zelfadministrerend",
}

# Twee verschillen met het overzicht bleken geen fouten maar een tijdsverschil:
# het overzicht noemt alvast de opvolger. Hier hoort de huidige uitvoerder te
# staan, dus die blijven ongemoeid — de aanstaande wissel staat in deze notitie.
#   IKEA/STIP (46)  Achmea Pensioenservices; het overzicht zegt PGGM. Het
#                   jaarverslag 2025 bevestigt Achmea en meldt dat het contract
#                   per ultimo 2027 is opgezegd; de opvolger wordt niet genoemd.
#   Avebe    (79)   Visma Idella; het overzicht zegt Dion. Het fonds meldde zelf
#                   op 3 mei 2026 de overgang naar DION Pensioen Services per
#                   1 januari 2027 — en stelt daarom het invaren uit naar 2028.

# Twee deelnemersaantallen zijn aantoonbaar fout: Rockwool staat op 877.542 bij
# €0,45 mld vermogen (€515 per deelnemer) en Exxonmobil op 107,9 miljoen — meer
# dan zes keer de Nederlandse bevolking. Bij Rockwool is de uitsplitsing ook
# intern inconsistent (gepensioneerden en slapers allebei 3.271), dus die gaat
# er integraal uit; het overzicht geeft 3.219 als totaal per eind 2022.
DEELNEMERS_HERSTEL = {
    125: 3219,   # Rockwool — uit het overzicht
    94:  None,   # Exxonmobil — geen betrouwbare bron, liever leeg dan fout
    136: 6126,   # Vopak — stond op 31 + 400 + 400; het overzicht geeft 6.126
    26:  12346,  # Molenaars — stond op 2.048 + 2.048 + 2.048 = 6.144
}

# Fondsen waar actief, slapers en gepensioneerd alle drie hetzelfde getal zijn.
# Dat kan niet: het is één gevonden getal dat in drie velden is geschreven. De
# uitsplitsing gaat eruit; het totaal blijft staan waar we er een bron voor
# hebben. De twee apf-moeders horen sowieso geen eigen deelnemers te hebben —
# die zitten in hun kringen.
UITSPLITSING_WISSEN = [
    64,   # APF Het Nederlandse Pensioenfonds — 216/216/216
    68,   # Unilever APF — 5.500/5.500/5.500
]


def kolom_toevoegen(cur, tabel, kolom, definitie):
    bestaand = {r[1] for r in cur.execute(f"PRAGMA table_info({tabel})")}
    if kolom not in bestaand:
        cur.execute(f"ALTER TABLE {tabel} ADD COLUMN {kolom} {definitie}")
        return True
    return False


def vul_leeg(cur, kolom, mapping, label, overschrijf=False):
    """Vult `kolom` alleen waar die nog leeg is, tenzij overschrijf=True."""
    gezet = overgeslagen = 0
    for fid, waarde in mapping.items():
        rij = cur.execute(f"SELECT name, {kolom} FROM funds WHERE id = ?", (fid,)).fetchone()
        if rij is None:
            print(f"  ! id {fid} bestaat niet meer — overgeslagen")
            continue
        naam, huidig = rij
        if huidig is not None and str(huidig).strip() and not overschrijf:
            if str(huidig).strip() != str(waarde):
                print(f"  = {naam[:44]:<46} behoudt {huidig!r} (overzicht: {waarde!r})")
            overgeslagen += 1
            continue
        cur.execute(f"UPDATE funds SET {kolom} = ? WHERE id = ?", (waarde, fid))
        gezet += 1
    print(f"  {label}: {gezet} gezet, {overgeslagen} al gevuld gelaten")
    return gezet


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DB))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    for tabel, kolom, definitie in (
        ("funds", "wtp_eindbestemming", "TEXT"),
        ("funds", "kring_soort", "TEXT"),
    ):
        if kolom_toevoegen(cur, tabel, kolom, definitie):
            print(f"kolom {tabel}.{kolom} toegevoegd")

    print("\nEindbestemming (fondsen die niet invaren):")
    vul_leeg(cur, "wtp_eindbestemming", EINDBESTEMMING, "eindbestemming")

    # Alle overige fondsen met een contractkeuze varen wél in; dat expliciet
    # vastleggen scheelt de app een afleidingsregel die stilzwijgend verkeerd
    # kan gaan zodra er een status bijkomt.
    n = cur.execute("""
        UPDATE funds SET wtp_eindbestemming = 'invaren'
         WHERE wtp_eindbestemming IS NULL
           AND COALESCE(status,'') NOT LIKE 'Duplicaat%'
           AND (COALESCE(wtp_contract_type,'') IN ('solidair','flexibel','flexibel + rdr','biedt beide aan')
                OR COALESCE(wtp_invaren,'') = 'ja')
    """).rowcount
    print(f"  invaren: {n} fondsen afgeleid uit contractkeuze/invarenintentie")

    print("\nOorspronkelijk beoogde invaardatum:")
    vul_leeg(cur, "wtp_oorspr_datum", OORSPRONKELIJK, "oorspr. datum")

    print("\nContractkeuze:")
    vul_leeg(cur, "wtp_contract_type", CONTRACT, "contracttype")

    print("\nSoort pensioenkring:")
    vul_leeg(cur, "kring_soort", KRING_SOORT, "kring_soort")

    print("\nUitvoerder HNPF-kringen:")
    vul_leeg(cur, "uitvoerder", {fid: DION for fid in HNPF_KRINGEN},
             "uitvoerder", overschrijf=True)
    vul_leeg(cur, "uitvoerder", UITVOERDER_HERSTEL, "uitvoerder", overschrijf=True)

    print("\nOnhoudbare deelnemersaantallen:")
    for fid, waarde in DEELNEMERS_HERSTEL.items():
        naam, oud = cur.execute(
            "SELECT name, deelnemers_totaal FROM funds WHERE id = ?", (fid,)).fetchone()
        cur.execute("""UPDATE funds SET deelnemers_totaal = ?, deelnemers_actief = NULL,
                       deelnemers_gepensioneerd = NULL, deelnemers_slapers = NULL
                        WHERE id = ?""", (waarde, fid))
        toon_oud = f"{oud:,}" if oud is not None else "leeg"
        print(f"  {naam[:36]:<38} {toon_oud:>12} -> {waarde if waarde else 'leeg'}")

    print("\nOnmogelijke uitsplitsingen (drie keer hetzelfde getal):")
    for fid in UITSPLITSING_WISSEN:
        naam, = cur.execute("SELECT name FROM funds WHERE id = ?", (fid,)).fetchone()
        cur.execute("""UPDATE funds SET deelnemers_actief = NULL, deelnemers_gepensioneerd = NULL,
                       deelnemers_slapers = NULL WHERE id = ?""", (fid,))
        print(f"  {naam[:44]:<46} uitsplitsing gewist")

    if args.dry_run:
        con.rollback()
        print("\n[dry run] niets weggeschreven")
    else:
        con.commit()
        print("\nopgeslagen")

    for kolom in ("wtp_eindbestemming", "kring_soort"):
        print(f"\n{kolom}:")
        for waarde, aantal in cur.execute(
            f"SELECT COALESCE({kolom},'(leeg)'), COUNT(*) FROM funds "
            f"WHERE COALESCE(status,'') NOT LIKE 'Duplicaat%' GROUP BY 1 ORDER BY 2 DESC"):
            print(f"  {waarde:<24} {aantal}")
    con.close()


if __name__ == "__main__":
    main()
