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
import re
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



def analyses_zonder_fonds(con):
    """Analyses die aan een fonds-id hangen dat niet (meer) in funds staat.

    Die zijn onzichtbaar op de site, want de dashboardquery joint op funds. Ze
    ontstaan als een fonds wordt samengevoegd of verwijderd terwijl de analyse
    blijft staan; er stonden er drie zo in de tabel.
    """
    return [f"fonds-id {fid} (boekjaar {jr}) bestaat niet in funds — bron: {os.path.basename(src or '?')}"
            for fid, jr, src in con.execute("""
                SELECT a.fund_id, a.fiscal_year, a.source_pdf FROM fund_analysis a
                LEFT JOIN funds f ON f.id = a.fund_id WHERE f.id IS NULL""")]


def analyses_op_duplicaatfonds(con):
    """Analyses bij een fonds dat als duplicaat is samengevoegd.

    Die verschijnen op de site als een apart fonds terwijl het dezelfde
    organisatie is; StiPP en PME stonden er zo dubbel in.
    """
    return [f"{naam} (id {fid}, boekjaar {jr}) — {status}"
            for fid, naam, jr, status in con.execute("""
                SELECT f.id, f.name, a.fiscal_year, f.status FROM fund_analysis a
                JOIN funds f ON f.id = a.fund_id
                WHERE COALESCE(f.status,'') LIKE 'Duplicaat%'""")]


def analyses_met_onmogelijk_boekjaar(con):
    """Boekjaar 0 of buiten een geloofwaardig bereik.

    Boekjaar 0 ontstond doordat het jaartal uit de bestandsnaam werd gehaald en
    de meeste PDF's dat niet in hun naam hebben.
    """
    return [f"fonds-id {fid}: boekjaar {jr}"
            for fid, jr in con.execute(
                "SELECT fund_id, fiscal_year FROM fund_analysis WHERE fiscal_year < 2010 OR fiscal_year > 2030")]


def analyses_uit_quarantaine(con):
    """Analyses die verwijzen naar een bestand in data/_broken/.

    Daar staan PDF's die geen jaarverslag bleken; een analyse mag daar niet meer
    op leunen.
    """
    return [f"fonds-id {fid} (boekjaar {jr}): {src}"
            for fid, jr, src in con.execute(
                "SELECT fund_id, fiscal_year, source_pdf FROM fund_analysis WHERE source_pdf LIKE '%_broken%'")]



def afbakening_afwijkend(con):
    """is_pensioenfonds wijkt af van wat de categorie zegt.

    De kolom wordt gezet door add_is_pensioenfonds.py op basis van categorie en
    een namenlijst. Wijkt hij later af, dan is er een categorie gewijzigd of een
    rij toegevoegd zonder de afbakening bij te werken — en dan telt een
    verzekeraar weer mee in het sectortotaal. Pensioenfonds Achmea stond zo als
    'Verzekeraar' geboekt en zou ten onrechte zijn weggefilterd.

    Een rij met een vastgelegde reden telt niet als afwijking. Sinds
    afbakening_reden bestaat staat daar waarom een rij is uitgesloten — een
    Belgische OFP-sectie, een duplicaat — en dat is geen slordigheid maar een
    besluit. Zonder deze uitzondering meldde de controle negen zulke rijen.
    """
    return [f"{naam}: categorie {cat!r} maar is_pensioenfonds={vlag}"
            for naam, cat, vlag in con.execute("""
                SELECT name, COALESCE(category,''), is_pensioenfonds FROM funds
                WHERE is_pensioenfonds IS NOT NULL
                  AND COALESCE(afbakening_reden,'') = ''
                  AND ((COALESCE(category,'') IN ('Verzekeraar','PPI') AND is_pensioenfonds = 1)
                    OR (COALESCE(category,'') NOT IN ('Verzekeraar','PPI')
                        AND is_pensioenfonds = 0))""")]


def historische_deelnemers_inconsistent(con):
    """Onmogelijke deelnemersaantallen in de jaarreeks.

    Alleen wat écht niet kan: een uitsplitsing die samen bóven het totaal
    uitkomt. Actief, slapers en gepensioneerden sluiten elkaar uit, dus dat is
    onmogelijk. Andersom kan wél — PMT heeft een totaal dat 4,5% boven de som
    ligt omdat arbeidsongeschikten buiten de drie kolommen vallen, en PGB heeft
    over 2021 een totaal van 342.150 bij een som van 219.487 doordat een kolom
    leeg is. Een eerdere versie meldde die dertig gevallen ook, en dan wordt een
    controle iets dat je wegklikt.
    """
    rijen = con.execute("""
        SELECT h.fund_id, f.name, h.year, h.deelnemers_actief a, h.deelnemers_slapers s,
               h.deelnemers_pensioengerechtigd g, h.deelnemers_totaal t
        FROM historical_metrics h JOIN funds f ON f.id = h.fund_id
        WHERE h.deelnemers_totaal > 0 AND h.deelnemers_actief IS NOT NULL
          AND h.deelnemers_slapers IS NOT NULL AND h.deelnemers_pensioengerechtigd IS NOT NULL
    """).fetchall()
    uit = []
    for r in rijen:
        som = r["a"] + r["s"] + r["g"]
        if som > r["t"] * 1.10:
            uit.append(f"{r['name'][:34]:36s} FY{r['year']}  "
                       f"{r['a']}+{r['s']}+{r['g']} = {som} ligt boven het totaal {r['t']}")
    return uit


def historische_deelnemers_gedeeld(con):
    """Dezelfde uitsplitsing bij twee of meer fondsen — bij hooguit een ervan echt.

    Arcadis, CK1 en CRH stonden alle drie op 2297/11142/533, en KLM Algemeen en
    KLM Cabinepersoneel deelden hun drietal. Zulke kopieën ontstaan wanneer een
    parser de tabel van het verkeerde hoofdstuk leest.
    """
    from collections import defaultdict
    per = defaultdict(list)
    for r in con.execute("""
            SELECT h.fund_id, f.name, h.year, h.deelnemers_actief a, h.deelnemers_slapers s,
                   h.deelnemers_pensioengerechtigd g
            FROM historical_metrics h JOIN funds f ON f.id = h.fund_id
            WHERE h.deelnemers_actief > 0 AND h.deelnemers_slapers > 0
              AND h.deelnemers_pensioengerechtigd > 0"""):
        per[(r["a"], r["s"], r["g"])].append((r["name"], r["year"]))
    uit = []
    for drietal, fondsen in per.items():
        namen = {n for n, _ in fondsen}
        if len(namen) > 1:
            uit.append(f"{'/'.join(str(x) for x in drietal):28s} bij "
                       + ", ".join(f"{n[:24]} FY{j}" for n, j in sorted(fondsen)))
    return uit


def pensioenfonds_zonder_dnb(con):
    """Als pensioenfonds gemarkeerd, vermogen van betekenis, maar DNB kent het niet.

    Elk Nederlands pensioenfonds rapporteert per kwartaal aan DNB; een gewoon
    fonds heeft hier vijfhonderd rijen. Nul rijen bij een vermogen van honderden
    miljoenen betekent dat het fonds ergens anders onder toezicht staat. Zo
    kwamen BP, ExxonMobil en J&J boven water: Nederlandse regelingen binnen een
    Belgische OFP, samen 5,8 miljard die meetelde in het sectortotaal.
    """
    rijen = con.execute(f"""
        SELECT f.id, f.name, COALESCE(f.aum_euro_bn, 0) aum FROM funds f
        WHERE {LEVEND} AND COALESCE(f.is_pensioenfonds, 1) = 1
          AND COALESCE(f.aum_euro_bn, 0) > 0.1
          AND NOT EXISTS (SELECT 1 FROM dnb_quarterly_metrics d WHERE d.fund_id = f.id)
        ORDER BY f.aum_euro_bn DESC""").fetchall()
    return [f"{r['name'][:38]:40s} {r['aum']:.2f} mrd, geen enkele DNB-kwartaalrij"
            for r in rijen]


# Grenzen waarbuiten een waarde geen meetfout meer is maar een verkeerde kolom.
UITSCHIETERS = [
    ("beleggingsrendement_pct", -50, 50, "rendement"),
    ("beleidsdekkingsgraad_pct", 50, 250, "beleidsdekkingsgraad"),
    ("nominale_dekkingsgraad_pct", 50, 300, "nominale dekkingsgraad"),
    ("reele_dekkingsgraad_pct", 30, 200, "reële dekkingsgraad"),
    ("indexatieverlening_pct", -25, 25, "indexatie"),
    ("cpi_pct", -5, 25, "prijsindex"),
    ("aum_euro_bn", 0.0001, 1000, "vermogen"),
]


def dekkingsgraad_na_invaren(con):
    """Een ingevaren fonds dat tóch een dekkingsgraad rapporteert.

    Onder de Wtp verdwijnt de dekkingsgraad; het vermogen staat op naam van de
    deelnemer. Staat er na het invaarmoment alsnog een waarde, dan komt die uit
    een oude bron of uit de verkeerde kolom. Andersom is een lege dekkingsgraad
    bij zo'n fonds juist correct, en geen gat dat gevuld moet worden.
    """
    rijen = con.execute("""
        SELECT f.name, h.year, h.nominale_dekkingsgraad_pct n, h.beleidsdekkingsgraad_pct b,
               f.invaardatum
        FROM historical_metrics h JOIN funds f ON f.id = h.fund_id
        WHERE f.invaardatum IS NOT NULL
          AND h.year > CAST(substr(f.invaardatum, 1, 4) AS INTEGER)
          AND (h.nominale_dekkingsgraad_pct IS NOT NULL OR h.beleidsdekkingsgraad_pct IS NOT NULL)
        ORDER BY f.name""").fetchall()
    return [f"{r['name'][:32]:34s} FY{r['year']} heeft nog een dekkingsgraad "
            f"({r['n'] or r['b']}) terwijl het fonds per {r['invaardatum']} is ingevaren"
            for r in rijen]


# Een deelnemer vertegenwoordigt grofweg tussen de duizend en vijf miljoen euro
# pensioenvermogen. ABP zit op ongeveer 175 duizend, een klein ondernemingsfonds
# op twee ton. De ondergrens ligt bewust laag: StiPP heeft 1,55 miljoen potjes
# van gemiddeld 2.838 euro, want uitzendkrachten bouwen kort op. Dat is echt en
# mag geen melding opleveren.
VERMOGEN_PER_DEELNEMER = (1_000, 5_000_000)


def vermogen_per_deelnemer_jaarreeks(con):
    """Vermogen en deelnemersaantal die niet bij elkaar kunnen horen.

    Rockwool stond over 2025 op 871.000 actieve deelnemers bij 438 miljoen euro,
    ofwel 499 euro per persoon, en HAL op 2,65 miljoen gepensioneerden bij 166
    miljoen. Beide waren intern consistent genoeg om aan de andere controles te
    ontsnappen: Rockwool telde netjes op tot zijn eigen totaal, en bij HAL was de
    kolom actief leeg waardoor de optelcontrole hem oversloeg. De verhouding tot
    het vermogen verraadt ze wel.
    """
    onder, boven = VERMOGEN_PER_DEELNEMER
    uit = []
    for r in con.execute(f"""
            SELECT f.name, h.year, COALESCE(f.aum_euro_bn, h.aum_euro_bn) aum,
                   MAX(COALESCE(h.deelnemers_totaal, 0),
                       COALESCE(h.deelnemers_actief, 0) + COALESCE(h.deelnemers_slapers, 0)
                       + COALESCE(h.deelnemers_pensioengerechtigd, 0)) n
            FROM historical_metrics h JOIN funds f ON f.id = h.fund_id
            WHERE {LEVEND} AND COALESCE(f.is_pensioenfonds, 1) = 1
            ORDER BY f.name, h.year"""):
        if not r["aum"] or not r["n"]:
            continue
        per = r["aum"] * 1e9 / r["n"]
        if not (onder <= per <= boven):
            uit.append(f"{r['name'][:30]:32s} FY{r['year']}  {r['aum']:.3f} mrd op {r['n']:,} "
                       f"deelnemers = {per:,.0f} euro per persoon".replace(",", "."))
    return uit


SFDR_BIJLAGE = re.compile(
    r"SFDR[- ]?(annex|bijlage)|periodieke informatieverschaffing"
    r"|bijlage\s+[IVX]+\s*[:.]?\s*SFDR|template.{0,20}periodic disclosure", re.I)


def sfdr_tegenstrijdig(con):
    """Als artikel 6 geboekt, maar het verslag bevat een periodieke SFDR-bijlage.

    De toets is bewust eenzijdig. Een bijlage aantreffen bij een fonds dat als
    artikel 8 of 9 staat, bevestigt dat — 49 fondsen kwamen zo door. Maar geen
    bijlage aantreffen bewijst niets, want veel fondsen publiceren die los van
    het jaarverslag; dat zou negentien valse meldingen opleveren, waaronder ABP.
    Andersom is wél sluitend: een artikel 6-product hoeft geen periodieke
    informatieverschaffing te publiceren, dus wie dat doet is er geen.
    """
    import glob as _glob
    uit = []
    for r in con.execute(f"""SELECT id, name, sfdr_article FROM funds
            WHERE {LEVEND} AND COALESCE(is_pensioenfonds, 1) = 1 AND sfdr_article = 6"""):
        bestanden = sorted(_glob.glob(os.path.join(
            BASE_DIR, "data", "annual_reports", f"{r['id']}_*.pdf")))
        if not bestanden:
            continue
        try:
            import fitz
            fitz.TOOLS.mupdf_display_errors(False)
            doc = fitz.open(bestanden[0])
            tekst = re.sub(r"\s+", " ", " ".join(p.get_text() for p in doc))
            doc.close()
        except Exception:
            continue
        if SFDR_BIJLAGE.search(tekst):
            uit.append(f"{r['name'][:38]:40s} staat als artikel 6 maar publiceert "
                       f"een periodieke SFDR-bijlage")
    return uit


def dubbele_nieuwsberichten(con):
    """Hetzelfde bericht meer dan eens, doordat de URL verschilt maar de inhoud niet.

    De tabel heeft UNIQUE op url, en dat is te fijnmazig: dezelfde pagina komt
    binnen als .../bericht en .../bericht#main, en een overzichtspagina zelfs met
    ?tag=Jaarverslag, ?tag=MVB en ?tag=jaarverslag naast elkaar. Dat leverde 552
    dubbele berichten op, zichtbaar als herhaalde regels op de nieuwspagina.
    """
    return [f"{r['name'][:30]:32s} {r['published_date'] or '(geen datum)'}  "
            f"{(r['title'] or '')[:44]} — {r['n']}x"
            for r in con.execute("""
                SELECT f.name, n.title, n.published_date, COUNT(*) n
                FROM news_articles n JOIN funds f ON f.id = n.fund_id
                GROUP BY n.fund_id, n.title, n.published_date
                HAVING COUNT(*) > 1 ORDER BY n DESC LIMIT 15""")]


def nieuwsdatum_ophoping(con):
    """Te veel berichten op precies dezelfde dag — dat is een noodwaarde.

    Een parser die geen datum vindt valt terug op een jaargrens, en dan staan er
    146 berichten op 31 december en 116 op 1 januari. Die zijn niet van elkaar te
    onderscheiden van een echte publicatie op die dag, en ze vervuilen elke
    sortering op datum. Zo kregen twee analyses een publicatiedatum van
    1 januari 2026 toegewezen terwijl hun verslag in juni verscheen.
    """
    return [f"{r['published_date']}: {r['n']} berichten op één dag"
            for r in con.execute("""
                SELECT published_date, COUNT(*) n FROM news_articles
                WHERE published_date IS NOT NULL
                GROUP BY published_date HAVING n >= 40 ORDER BY n DESC""")]


def uitschieters_jaarreeks(con):
    """Waarden buiten elk redelijk bereik in de jaarreeks.

    Zo kwam Gasunie boven water: vijf 'indexaties' van 121 tot 137 procent, die
    in werkelijkheid nominale dekkingsgraden waren. De kolom ernaast stond leeg,
    dus de parser had ze een plek opgeschoven. Een toeslag van 132 procent bestaat
    niet, en dat is precies wat zo'n grens zichtbaar maakt.
    """
    uit = []
    for kolom, onder, boven, label in UITSCHIETERS:
        for r in con.execute(f"""
                SELECT f.name, h.year, h.{kolom} w FROM historical_metrics h
                JOIN funds f ON f.id = h.fund_id
                WHERE h.{kolom} IS NOT NULL AND h.{kolom} NOT BETWEEN ? AND ?
                ORDER BY f.name LIMIT 12""", (onder, boven)):
            uit.append(f"{r['name'][:32]:34s} FY{r['year']}  {label} = {r['w']}")
    return uit


CONTROLES = [
    ("Deelnemers tellen niet op tot het totaal", deelnemers_inconsistent),
    ("Deelnemers in de jaarreeks liggen boven het totaal", historische_deelnemers_inconsistent),
    ("Zelfde deelnemersuitsplitsing in de jaarreeks bij meerdere fondsen",
     historische_deelnemers_gedeeld),
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
    ("Analyses bij een fonds dat niet in funds staat", analyses_zonder_fonds),
    ("Analyses bij een als duplicaat samengevoegd fonds", analyses_op_duplicaatfonds),
    ("Analyses met een onmogelijk boekjaar", analyses_met_onmogelijk_boekjaar),
    ("Analyses die leunen op een bestand in quarantaine", analyses_uit_quarantaine),
    ("Afbakening pensioenfonds wijkt af van de categorie", afbakening_afwijkend),
    ("Als pensioenfonds gemarkeerd maar onbekend bij DNB", pensioenfonds_zonder_dnb),
    ("Waarden buiten elk redelijk bereik in de jaarreeks", uitschieters_jaarreeks),
    ("Vermogen en deelnemers in de jaarreeks passen niet bij elkaar",
     vermogen_per_deelnemer_jaarreeks),
    ("Dekkingsgraad gerapporteerd na het invaren", dekkingsgraad_na_invaren),
    ("SFDR-artikel spreekt het eigen verslag tegen", sfdr_tegenstrijdig),
    ("Hetzelfde nieuwsbericht meer dan eens opgeslagen", dubbele_nieuwsberichten),
    ("Nieuwsdatums die op één dag ophopen", nieuwsdatum_ophoping),
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
