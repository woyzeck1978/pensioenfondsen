"""Laad de uitgelezen inrichting van de aandelenstrategie (verslagcijfers/<id>.json) in drie afgeleide tabellen.

equity_setup_2025            één rij per fonds: allocatie, organisatie, stijl, kosten, ESG, valuta (+ bron en pagina's)
equity_setup_beheerders_2025 één rij per genoemde beheerder/mandaat
equity_setup_regio_2025      één rij per regio-gewicht

De tabellen worden elke keer opnieuw opgebouwd (afgeleid, herleidbaar naar de JSON). Standaard alleen een rapport
van de verschillen met de bestaande velden; met --schrijf worden de tabellen geschreven. Bestaande kolommen in
`funds`/`equity_strategy` blijven onaangeroerd.
"""
import glob, json, os, re, sqlite3, sys

S = os.path.dirname(os.path.abspath(__file__))
DB = os.path.expanduser("~/pensioenfondsen-app/data/processed/pension_funds.db")
SCHRIJF = "--schrijf" in sys.argv

# Beheerdersnamen zoals de verslagen ze schrijven -> één naam per partij
NORM = [
    (r"\bdps\b|dsm pension", "DPS (DSM Pension Services)"),
    (r"black ?rock|blk\b", "BlackRock"), (r"northern trust", "Northern Trust"), (r"state street|ssga", "State Street"),
    (r"\bapg\b", "APG"), (r"\bpggm\b", "PGGM"), (r"\bmn\b|mn services", "MN"), (r"achmea", "Achmea IM"),
    (r"kempen|van lanschot", "Van Lanschot Kempen"), (r"goldman|gsam", "Goldman Sachs AM"), (r"aegon|a\.?e\.?g\.?o\.?n", "Aegon AM"),
    (r"cardano", "Cardano"), (r"amundi", "Amundi"), (r"vanguard", "Vanguard"), (r"\bdws\b", "DWS"), (r"\bubs\b", "UBS"),
    (r"\baxa\b", "AXA IM"), (r"legal & general|lgim|l&g", "LGIM"), (r"columbia|threadneedle", "Columbia Threadneedle"),
    (r"russell", "Russell Investments"), (r"nn ip|nn investment|nationale.nederlanden", "NN IP / Goldman Sachs"),
    (r"bnp", "BNP Paribas AM"), (r"schroder", "Schroders"), (r"baillie", "Baillie Gifford"), (r"wellington", "Wellington"),
    (r"dimensional|\bdfa\b", "Dimensional"), (r"\btkp\b", "TKP"), (r"a\.?s\.?r\.?|asr vermogensbeheer", "a.s.r."),
    (r"invesco", "Invesco"), (r"fidelity", "Fidelity"), (r"t\. ?rowe", "T. Rowe Price"), (r"j\.?p\.? ?morgan", "J.P. Morgan AM"),
    (r"lazard", "Lazard"), (r"insight", "Insight"), (r"pimco", "PIMCO"), (r"robeco", "Robeco"), (r"capital group", "Capital Group"),
    (r"alliance ?bernstein", "AllianceBernstein"), (r"morgan stanley", "Morgan Stanley IM"), (r"comgest", "Comgest"),
    (r"acadian", "Acadian"), (r"arrowstreet", "Arrowstreet"), (r"jpm", "J.P. Morgan AM"), (r"triodos", "Triodos"),
    (r"actiam", "ACTIAM"), (r"anthos", "Anthos"), (r"\bing\b", "ING"), (r"\bsei\b", "SEI"), (r"man group|man numeric", "Man Group"),
    (r"nomura", "Nomura"), (r"hermes|federated", "Federated Hermes"), (r"ubs", "UBS"), (r"natixis|ossiam|mirova", "Natixis IM"),
    (r"allianz|allianzgi", "Allianz GI"), (r"abn amro", "ABN AMRO IS"), (r"osmosis", "Osmosis"), (r"impax", "Impax"),
    (r"skagen|storebrand", "Storebrand"), (r"candriam", "Candriam"), (r"nn group", "NN"), (r"\bmm\b|multi.manager", "Aegon AM"), (r"\baam\b", "Aegon AM"), (r"u-fcp|univest", "Univest (Unilever)"),
    (r"towers watson|\bwtw\b", "WTW"), (r"pensioendiensten|uitvoeringsorganisatie|bestuursbureau", "intern"),
    (r"pensioenfonds|het fonds|\bintern\b|\beigen\b", "intern"),
]


def norm(naam):
    laag = naam.lower()
    for pat, n in NORM:
        if re.search(pat, laag):
            return n
    return naam.strip()


def lijst(x):
    return "; ".join(x) if x else None


con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row
rijen, beheerders, regios = [], [], []
for pad in sorted(glob.glob(os.path.join(S, "verslagcijfers", "*.json")), key=lambda p: int(os.path.basename(p)[:-5])):
    v = json.load(open(pad))
    fid = v["fund_id"]
    a, b, st, k, e, r, va = (v[x] for x in ("allocatie", "beheer", "stijl", "kosten", "esg", "regio", "valuta"))
    berekend = (round(100 * a["feitelijk_eur_mln"] / a["totaal_beleggingen_eur_mln"], 1)
                if a["feitelijk_eur_mln"] and a["totaal_beleggingen_eur_mln"] else None)
    paginas = {x: v[x].get("pagina") for x in ("allocatie", "beheer", "stijl", "kosten", "esg", "regio", "valuta")}
    rijen.append((fid, a["feitelijk_pct"], a["feitelijk_eur_mln"], a["totaal_beleggingen_eur_mln"], berekend,
                  a["strategisch_pct"], a["strategisch_basis"], a["private_equity_pct"], a["omvat_private_equity"],
                  b["organisatie"], b["fiduciair_beheerder"], norm(b["fiduciair_beheerder"]) if b["fiduciair_beheerder"] else None,
                  len(b["beheerders"]),
                  st["hoofdstijl"], st["passief_pct"], st["actief_pct"], st["factorbeleggen"], lijst(st["benchmarks"]), st["tracking_error_pct"],
                  k["beheerkosten_aandelen_pct"], k["transactiekosten_aandelen_pct"], k["prestatievergoeding_aandelen_eur_mln"],
                  k["vermogensbeheerkosten_totaal_pct"], k["transactiekosten_totaal_pct"],
                  lijst(e["instrumenten"]), e["co2_doel"], e["sfdr_artikel"], e["eigen_index"], e["omschrijving"],
                  r["basis"], va["aandelen_afgedekt_pct"], va["omschrijving"], v["opmerkingen"],
                  v["pad"], v.get("bron_model"), json.dumps(paginas)))
    for m in b["beheerders"]:
        beheerders.append((fid, m["naam"], norm(m["naam"]), m.get("product_of_mandaat"), m["stijl"], m.get("segment"), m.get("pagina")))
    for w in r["verdeling"]:
        regios.append((fid, w["regio"], w["pct"], r["basis"]))

# --- vergelijking met wat er al stond -------------------------------------------------------------
print(f"{len(rijen)} fondsen gelezen; {len(beheerders)} beheerdersregels; {len(regios)} regiogewichten\n")
tel = {"alloc_feitelijk": 0, "alloc_strategisch": 0, "alloc_geen": 0, "stijl_gelijk": 0, "stijl_anders": 0, "stijl_nieuw": 0,
       "kosten_nieuw": 0, "kosten_gelijk": 0, "kosten_anders": 0, "fid_nieuw": 0}
verschillen = []
for rij in rijen:
    fid, feit, _, _, ber, strat = rij[:6]
    oud = con.execute("select equity_allocation_pct, fiduciair_beheerder, equity_beheerkosten_pct, name from funds where id=?", (fid,)).fetchone()
    if oud is None:
        continue
    beste = feit if feit is not None else ber
    if beste is not None:
        tel["alloc_feitelijk"] += 1
    elif strat is not None:
        tel["alloc_strategisch"] += 1
    else:
        tel["alloc_geen"] += 1
    if oud["equity_allocation_pct"] is not None and beste is not None and abs(oud["equity_allocation_pct"] - beste) > 3 \
            and (strat is None or abs(oud["equity_allocation_pct"] - strat) > 3):
        verschillen.append((fid, oud["name"], "allocatie", oud["equity_allocation_pct"], beste, strat))
    es = con.execute("select management_style from equity_strategy where fund_id=?", (fid,)).fetchone()
    nieuw = {"passief": "Passief", "actief": "Actief", "gemengd": "Gemengd"}.get(rij[13])
    if nieuw:
        if es is None or not es["management_style"]:
            tel["stijl_nieuw"] += 1
        elif es["management_style"] == nieuw:
            tel["stijl_gelijk"] += 1
        else:
            tel["stijl_anders"] += 1
            verschillen.append((fid, oud["name"], "beheerstijl", es["management_style"], nieuw, None))
    kn = rij[19]
    if kn is not None:
        if oud["equity_beheerkosten_pct"] is None:
            tel["kosten_nieuw"] += 1
        elif abs(oud["equity_beheerkosten_pct"] - kn) <= 0.02:
            tel["kosten_gelijk"] += 1
        else:
            tel["kosten_anders"] += 1
            verschillen.append((fid, oud["name"], "beheerkosten", oud["equity_beheerkosten_pct"], kn, None))
    if rij[10] and not oud["fiduciair_beheerder"]:
        tel["fid_nieuw"] += 1
for k_, v_ in tel.items():
    print(f"  {k_:18s} {v_}")
print()
for fid, naam, veld, oud_, nieuw_, extra in verschillen:
    print(f"  {fid:4d} {naam[:40]:40s} {veld:12s} db={oud_} verslag={nieuw_}" + (f" strategisch={extra}" if extra else ""))

if not SCHRIJF:
    print("\n(geen --schrijf: niets geschreven)")
    sys.exit()

con.executescript("""
drop table if exists equity_setup_2025; drop table if exists equity_setup_beheerders_2025; drop table if exists equity_setup_regio_2025;
create table equity_setup_2025 (
  fund_id integer primary key references funds(id),
  feitelijk_pct real, feitelijk_eur_mln real, totaal_beleggingen_eur_mln real, berekend_pct real,
  strategisch_pct real, strategisch_basis text, private_equity_pct real, omvat_private_equity integer,
  organisatie text, fiduciair_beheerder text, fiduciair_norm text, aantal_beheerders integer,
  hoofdstijl text, passief_pct real, actief_pct real, factorbeleggen integer, benchmarks text, tracking_error_pct real,
  beheerkosten_aandelen_pct real, transactiekosten_aandelen_pct real, prestatievergoeding_aandelen_eur_mln real,
  vermogensbeheerkosten_totaal_pct real, transactiekosten_totaal_pct real,
  esg_instrumenten text, co2_doel text, sfdr_artikel integer, eigen_index integer, esg_omschrijving text,
  regio_basis text, valuta_afgedekt_pct real, valuta_omschrijving text, opmerkingen text,
  bron_pdf text, bron_model text, paginas_json text, generated_at timestamp default current_timestamp);
create table equity_setup_beheerders_2025 (
  id integer primary key autoincrement, fund_id integer references funds(id), naam text, naam_norm text,
  product_of_mandaat text, stijl text, segment text, pagina integer);
create table equity_setup_regio_2025 (fund_id integer references funds(id), regio text, pct real, basis text, primary key (fund_id, regio));
""")
con.executemany("insert into equity_setup_2025 values (" + ",".join("?" * 36) + ", current_timestamp)", rijen)
con.executemany("insert into equity_setup_beheerders_2025 (fund_id, naam, naam_norm, product_of_mandaat, stijl, segment, pagina) values (?,?,?,?,?,?,?)", beheerders)
con.executemany("insert or replace into equity_setup_regio_2025 values (?,?,?,?)", regios)
con.commit()
print("\ngeschreven:", len(rijen), "fondsen,", len(beheerders), "beheerders,", len(regios), "regiogewichten")
