"""Leg de uit de verslagen gelezen cijfers over 2025 (en 2024) naast historical_metrics.

Standaard alleen een rapport. Met --schrijf worden afwijkingen vervangen door de
verslagwaarde, lege velden gevuld, en deelnemersvelden die in de database een
patroon van een leesfout dragen maar niet uit het verslag te bevestigen zijn
leeggemaakt. Elke wijziging komt met pagina en letterlijke vindplaats in het log.
"""
import collections, csv, glob, json, os, sqlite3, sys

S = os.path.dirname(os.path.abspath(__file__))
DB = os.path.expanduser("~/pensioenfondsen-app/data/processed/pension_funds.db")
SCHRIJF = "--schrijf" in sys.argv
LOG = os.path.join(S, "correctielog_2025.csv")

PCT = ["beleggingsrendement_pct", "beleidsdekkingsgraad_pct", "nominale_dekkingsgraad_pct",
       "reele_dekkingsgraad_pct", "vereiste_dekkingsgraad_pct", "rente_afdekking_pct"]
AANTAL = ["deelnemers_actief", "deelnemers_slapers", "deelnemers_pensioengerechtigd", "deelnemers_totaal"]
VELDEN = PCT + ["aum_euro_bn"] + AANTAL


def gelijk(veld, a, b):
    if veld in PCT:
        return abs(a - b) <= 0.15
    if veld == "aum_euro_bn":
        return abs(a - b) <= max(0.002, 0.02 * max(abs(a), abs(b)))
    return abs(a - b) <= max(2, 0.01 * max(a, b))


def verdacht_deelnemers(rij):
    a, s, g = rij["deelnemers_actief"], rij["deelnemers_slapers"], rij["deelnemers_pensioengerechtigd"]
    return any(x is not None and x == y and x > 100 for x, y in ((a, s), (a, g), (s, g)))


con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row
wijzigingen, tellers = [], {"gelijk": 0, "gevuld": 0, "vervangen": 0, "leeggemaakt": 0, "niet_in_verslag": 0}

for pad in sorted(glob.glob(os.path.join(S, "verslagcijfers", "*.json")), key=lambda p: int(os.path.basename(p)[:-5])):
    v = json.load(open(pad))
    fid = v["fund_id"]
    naam = con.execute("select name from funds where id=?", (fid,)).fetchone()[0]
    for jaar in (2025, 2024):
        rij = con.execute("select * from historical_metrics where fund_id=? and year=?", (fid, jaar)).fetchone()
        verdacht = rij is not None and verdacht_deelnemers(rij)
        for veld in VELDEN:
            info = v["velden"].get(veld) or {}
            nieuw = info.get(str(jaar))
            oud = rij[veld] if rij is not None else None
            if nieuw is None:
                if oud is not None and veld in AANTAL and verdacht:
                    wijzigingen.append((fid, naam, jaar, veld, oud, None, "leeggemaakt", None,
                                        "leesfoutpatroon, niet te bevestigen uit het verslag"))
                    tellers["leeggemaakt"] += 1
                elif oud is not None:
                    tellers["niet_in_verslag"] += 1
                continue
            if oud is not None and gelijk(veld, oud, nieuw):
                tellers["gelijk"] += 1
                continue
            soort = "gevuld" if oud is None else "vervangen"
            # 2024 alleen vullen of vervangen als het verslag over 2025 het expliciet als vergelijkend cijfer geeft;
            # het verslag over 2024 blijft de primaire bron, dus daar alleen afwijkingen melden.
            if jaar == 2024 and soort == "vervangen":
                soort = "afwijking_2024"
            # Renteafdekking en belegd vermogen hebben per verslag een andere definitie
            # (strategisch tegen gemeten, bruto tegen netto na derivaten). Een verschil
            # daar is vaak geen leesfout; alleen een evidente sprong wordt vervangen.
            if soort == "vervangen" and veld == "rente_afdekking_pct":
                soort = "ter_beoordeling"
            if soort == "vervangen" and veld == "aum_euro_bn" and max(oud, nieuw) / max(min(oud, nieuw), 1e-9) < 1.5:
                soort = "ter_beoordeling"
            wijzigingen.append((fid, naam, jaar, veld, oud, nieuw, soort, info.get("pagina"), info.get("letterlijk")))
            tellers[soort] = tellers.get(soort, 0) + 1

with open(LOG, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["fund_id", "fonds", "jaar", "veld", "database", "verslag", "actie", "pagina", "vindplaats"])
    w.writerows(wijzigingen)

print(tellers)
for r in wijzigingen:
    if r[6] in ("vervangen", "leeggemaakt"):
        print(f"{r[0]:>4} {r[1][:28]:28} {r[2]} {r[3]:30} {r[4]!s:>10} -> {r[5]!s:<10} {r[6]} p.{r[7]}")

datums = []
for pad in glob.glob(os.path.join(S, "verslagcijfers", "*.json")):
    v = json.load(open(pad))
    d = (v.get("vaststellingsdatum") or {}).get("datum")
    if d and d[:4] == "2026":
        huidig = con.execute("select source_published_date from fund_analysis where fund_id=? and fiscal_year=2025",
                             (v["fund_id"],)).fetchone()
        if huidig is not None and not huidig[0]:
            datums.append((d, v["fund_id"]))
print(len(datums), "publicatiedatums te vullen uit de vaststellingsdatum")

# Tweede lezing door agy (Gemini): alleen wat daar als voorstel_klopt terugkomt gaat de database in.
oordeel = {}
for f in glob.glob(os.path.join(S, "tweede_lezing", "uit_*.json")):
    for it in json.load(open(f)).get("items", []):
        oordeel[it["nr"]] = it["oordeel"]

import re, subprocess
BASE = os.path.expanduser("~/pensioenfondsen-app")
paden = {}
for f in glob.glob(os.path.join(S, "verslagcijfers", "*.json")):
    v = json.load(open(f))
    paden[v["fund_id"]] = v["pad"]
_pagina = {}


def staat_op_pagina(fid, veld, waarde, pagina):
    """Het getal moet letterlijk op de opgegeven pagina (of een buurpagina) staan, in NL- of EN-notatie."""
    if not pagina or fid not in paden:
        return False
    p = int(float(pagina))
    tekst = ""
    for q in (p - 1, p, p + 1):
        if q < 1:
            continue
        if (fid, q) not in _pagina:
            _pagina[(fid, q)] = subprocess.run(["pdftotext", "-layout", "-f", str(q), "-l", str(q),
                                                os.path.join(BASE, paden[fid]), "-"], capture_output=True, text=True).stdout
        tekst += _pagina[(fid, q)]
    tekst = re.sub(r"\s+", " ", tekst)
    v = float(waarde)
    kandidaten = set()
    if veld == "aum_euro_bn":
        for mult in (1e3, 1e6, 1e9, 1):
            x = v * mult
            for dec in (0, 1, 2, 3):
                kandidaten.add(f"{x:,.{dec}f}")
    elif veld.startswith("deelnemers"):
        kandidaten.add(f"{v:,.0f}")
        kandidaten.add(f"{v:.0f}")
    else:
        for dec in (0, 1, 2):
            kandidaten.add(f"{abs(v):.{dec}f}")
    alle = set()
    for k in kandidaten:
        alle.add(k)
        alle.add(k.replace(",", "X").replace(".", ",").replace("X", "."))
    return any(re.search(r"(?<![\d.,])" + re.escape(k) + r"(?![\d])", tekst) for k in alle if k not in ("0", "0.0", "0,0"))


toepassen, niet = [], collections.Counter()
for w in wijzigingen:
    fid, _n, jaar, veld, _oud, nieuw, actie, _p, _l = w
    nr = f"{fid}-{veld}"
    if actie == "leeggemaakt":
        toepassen.append(w)
    elif jaar == 2025 and actie in ("gevuld", "vervangen"):
        if oordeel.get(nr) != "voorstel_klopt":
            niet[oordeel.get(nr, "niet_nagelezen")] += 1
        elif not staat_op_pagina(fid, veld, nieuw, _p):
            niet["niet_letterlijk_op_pagina"] += 1
        else:
            toepassen.append(w)
    elif jaar == 2024 and actie == "gevuld":
        # Een vergelijkend cijfer uit dezelfde tabelregel: alleen als die regel over 2025 bevestigd is.
        if oordeel.get(nr) == "voorstel_klopt" or (oordeel.get(nr) is None and any(
                x[0] == fid and x[2] == 2025 and x[3] == veld for x in toepassen)):
            toepassen.append(w)
        else:
            niet["2024_zonder_bevestigde_regel"] += 1
    else:
        niet[actie] += 1
print("toe te passen:", len(toepassen), "| niet toegepast:", dict(niet))

with open(os.path.join(S, "ter_beoordeling_2025.csv"), "w", newline="") as f:
    w_ = csv.writer(f)
    w_.writerow(["fund_id", "fonds", "jaar", "veld", "database", "verslag", "actie", "pagina", "vindplaats", "tweede_lezing"])
    for w in wijzigingen:
        if w not in toepassen and w[6] != "afwijking_2024":
            w_.writerow(list(w) + [oordeel.get(f"{w[0]}-{w[3]}", "niet_nagelezen")])

if SCHRIJF:
    with open(os.path.join(S, "toegepast_2025.csv"), "w", newline="") as f:
        w_ = csv.writer(f)
        w_.writerow(["fund_id", "fonds", "jaar", "veld", "database", "verslag", "actie", "pagina", "vindplaats"])
        w_.writerows(toepassen)
    with con:
        con.executemany("update fund_analysis set source_published_date=? where fund_id=? and fiscal_year=2025", datums)
        for fid, _n, jaar, veld, _oud, nieuw, actie, _p, _l in toepassen:
            if not con.execute("select 1 from historical_metrics where fund_id=? and year=?", (fid, jaar)).fetchone():
                con.execute("insert into historical_metrics (fund_id, year) values (?, ?)", (fid, jaar))
            con.execute(f"update historical_metrics set {veld}=? where fund_id=? and year=?", (nieuw, fid, jaar))
    print("geschreven; log:", LOG)
