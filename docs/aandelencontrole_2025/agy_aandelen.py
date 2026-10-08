"""Laat agy de inrichting van de aandelenstrategie uit geknipte verslagtekst lezen en schrijf verslagcijfers/<id>.json.

Invoer: een index-bestand (lijst met tekst, pad, fondsen) uit aandelen_prep.py.
Fondsen die al een verslagcijfers-bestand hebben worden overgeslagen. Taken lopen parallel.
Aanroep: agy_aandelen.py <index.json> [parallel=3]
"""
import json, os, subprocess, sys
from concurrent.futures import ThreadPoolExecutor

S = os.path.dirname(os.path.abspath(__file__))
DELEGATE = "/Users/webkowuite/Library/Mobile Documents/com~apple~CloudDocs/R/.claude/skills/delegate/delegate.sh"
MAP = os.path.join(S, "agy_aandelen")
os.makedirs(MAP, exist_ok=True)
getal = {"type": ["number", "null"]}
tekst = {"type": ["string", "null"]}
pagina = {"type": ["integer", "null"]}


def obj(**velden):
    return {"type": "object", "additionalProperties": False, "required": list(velden), "properties": velden}


def blok(**velden):
    return obj(**velden, pagina=pagina, letterlijk={"type": "string"})


def enum(*w):
    return {"type": "string", "enum": list(w)}


REGIO = ["Noord-Amerika", "Europa", "Azië-Pacific", "Opkomende markten", "Wereld ontwikkeld", "Nederland", "Overig"]
ESG_INSTR = ["uitsluitingen", "best-in-class", "engagement", "stembeleid", "impactbeleggen", "eigen ESG-index",
             "CO2-reductiedoel", "Paris-aligned benchmark", "ESG-integratie in selectie"]
FONDS = obj(
    fund_id={"type": "integer"},
    allocatie=blok(feitelijk_pct=getal, feitelijk_eur_mln=getal, totaal_beleggingen_eur_mln=getal, strategisch_pct=getal,
                   strategisch_basis=enum("totale portefeuille", "rendementsportefeuille", "onbekend", "niet genoemd"),
                   private_equity_pct=getal, omvat_private_equity={"type": ["boolean", "null"]}),
    beheer=blok(organisatie=enum("intern", "extern", "gemengd", "onbekend"), fiduciair_beheerder=tekst,
                beheerders={"type": "array", "items": obj(naam={"type": "string"}, product_of_mandaat=tekst,
                                                           stijl=enum("passief", "actief", "enhanced/factor", "onbekend"),
                                                           segment=tekst, pagina=pagina)}),
    stijl=blok(hoofdstijl=enum("passief", "actief", "gemengd", "onbekend"), passief_pct=getal, actief_pct=getal,
               factorbeleggen={"type": ["boolean", "null"]}, benchmarks={"type": "array", "items": {"type": "string"}},
               tracking_error_pct=getal),
    kosten=blok(beheerkosten_aandelen_pct=getal, transactiekosten_aandelen_pct=getal, prestatievergoeding_aandelen_eur_mln=getal,
                vermogensbeheerkosten_totaal_pct=getal, transactiekosten_totaal_pct=getal),
    esg=blok(instrumenten={"type": "array", "items": enum(*ESG_INSTR)}, co2_doel=tekst, sfdr_artikel={"type": ["integer", "null"]},
             eigen_index={"type": ["boolean", "null"]}, omschrijving={"type": "string"}),
    regio=blok(basis=enum("feitelijk", "strategisch", "benchmark", "onbekend", "niet genoemd"),
               verdeling={"type": "array", "items": obj(regio=enum(*REGIO), pct=getal)}),
    valuta=blok(aandelen_afgedekt_pct=getal, omschrijving={"type": "string"}),
    opmerkingen={"type": "string"})
SCHEMA = obj(fondsen={"type": "array", "items": FONDS})
schema_pad = os.path.join(MAP, "schema.json")
json.dump(SCHEMA, open(schema_pad, "w"))

OPDRACHT = """# Opdracht
Je beschrijft hoe een Nederlands pensioenfonds zijn AANDELENbeleggingen heeft ingericht, op basis van het jaarverslag 2025.
Van elk verslag krijg je een uitsnede van de relevante pagina's, gemarkeerd als "===== PDF-pagina N =====". Geef per fund_id
één object. Bij een verslag dat meerdere fondsen (kringen) dekt: geef per kring wat voor die kring geldt.

Het gaat uitsluitend om de aandelenportefeuille (beursgenoteerde aandelen), niet om vastrentende waarden, vastgoed of de
hele portefeuille — behalve waar dat expliciet gevraagd wordt (totaalkosten, fiduciair beheerder).

Velden (null, lege lijst of "niet genoemd" als het verslag het niet noemt; verzin niets, schat niets, reken niets zelf uit):
- allocatie: feitelijk_pct = gewicht van aandelen in % van de totale beleggingen ultimo 2025 zoals het verslag het noemt;
  feitelijk_eur_mln en totaal_beleggingen_eur_mln uit dezelfde tabel (balans of "samenstelling beleggingen"), in MILJOENEN
  euro (reken duizenden om, noem de eenheid in letterlijk); strategisch_pct = normgewicht aandelen (strategische mix,
  normportefeuille) met basis "totale portefeuille" of "rendementsportefeuille"; private_equity_pct apart;
  omvat_private_equity = true als de aandelenpost aantoonbaar ook private equity bevat.
- beheer: organisatie = intern (eigen beleggingsorganisatie, bv. APG/PGGM/MN voor hun eigen fondsen), extern of gemengd;
  fiduciair_beheerder = de fiduciair/integraal beheerder van het hele fonds; beheerders = elke genoemde partij die (een
  deel van) de aandelen beheert, met product_of_mandaat (naam van het fonds/pool/mandaat, bv. "BlackRock MSCI World
  Index Fund", "Northern Trust Emerging Markets Custom ESG Equity Index Fund"), stijl en segment (bv. "wereldwijd
  ontwikkeld", "opkomende markten", "small caps", "Europa").
- stijl: hoofdstijl van de aandelenportefeuille; passief_pct/actief_pct = genoemd aandeel passief/actief beheerd in %;
  factorbeleggen = true bij factor-, smart-beta- of enhanced-index-strategieën; benchmarks = genoemde indices voor
  aandelen (bv. "MSCI World", "MSCI ACWI Custom ESG"); tracking_error_pct indien genoemd.
- kosten: beheerkosten_aandelen_pct en transactiekosten_aandelen_pct = kosten van de categorie aandelen in % van het
  belegd vermogen in aandelen (uit de tabel "kosten per beleggingscategorie"; basispunten omrekenen: 12 bp -> 0.12);
  prestatievergoeding_aandelen_eur_mln; vermogensbeheerkosten_totaal_pct en transactiekosten_totaal_pct = fondsbrede
  kostenpercentages 2025.
- esg: instrumenten die het fonds op aandelen toepast; co2_doel letterlijk (bv. "-50% in 2030 t.o.v. 2019");
  sfdr_artikel (6, 8 of 9); eigen_index = true bij een eigen/maatwerk ESG-index; omschrijving kort.
- regio: verdeling van de aandelenportefeuille over regio's in %, met basis (feitelijk ultimo 2025, strategisch of
  benchmark). Gebruik alleen de gegeven regionamen; "Wereld ontwikkeld" als het verslag alleen ontwikkeld vs opkomend
  onderscheidt. Gewichten zoals genoemd, niet hernormaliseren.
- valuta: afdekking van valutarisico in de aandelenportefeuille (percentage en korte omschrijving).
- opmerkingen: wat verder opvalt over de inrichting (wijzigingen in 2025, nieuwe mandaten, Wtp-verband, voorbehouden).

Regels: tabellen tonen meerdere jaren naast elkaar, LEES DE KOLOMKOP en neem 2025. `pagina` = het PDF-paginanummer uit de
markering. `letterlijk` = korte letterlijke tekst waar het belangrijkste gegeven van dat blok staat (leeg als niets).
Getallen met punt als decimaalteken (38,5% -> 38.5; 4.469 -> 4469).

# Uitvoer
JSON volgens het schema: {"fondsen": [ ... ]}.

# Verslagen
"""

index = json.load(open(sys.argv[1]))
PARALLEL = int(sys.argv[2]) if len(sys.argv) > 2 else 3
te_doen = [x for x in index if x.get("tekst") and not all(
    os.path.exists(os.path.join(S, "verslagcijfers", f"{f}.json")) for f, _ in x["fondsen"])]


def draai(x):
    eerste = x["fondsen"][0][0]
    taak = os.path.join(MAP, f"taak_{eerste}.md")
    uit = os.path.join(MAP, f"uit_{eerste}.json")
    with open(taak, "w") as f:
        f.write(OPDRACHT)
        f.write(f"\n## Verslag voor fund_id(s): {', '.join(f'{fid} ({n})' for fid, n in x['fondsen'])}\n```\n")
        f.write(open(x["tekst"]).read())
        f.write("\n```\n")
    r = subprocess.run(["bash", DELEGATE, "--model", "gemini-3.8-flash-high", "--out", uit, "--schema", schema_pad,
                        "--timeout", "10m", taak], capture_output=True, text=True)
    print(f"taak {os.path.basename(taak)}: exit {r.returncode} {r.stderr.strip()[-120:]}", flush=True)
    if r.returncode == 3:
        return "quotum"
    if r.returncode not in (0, 4) or not os.path.exists(uit):
        return None
    try:
        res = json.load(open(uit))
    except Exception as e:
        print("  onleesbare uitvoer:", e, flush=True)
        return None
    toegestaan = {fid: (n, x["pad"]) for fid, n in x["fondsen"]}
    for fo in res.get("fondsen", []):
        if fo["fund_id"] not in toegestaan:
            print("  onbekend fund_id genegeerd:", fo["fund_id"], flush=True)
            continue
        naam, pad = toegestaan[fo["fund_id"]]
        fo.update({"naam": naam, "pad": pad, "bron_model": "agy gemini-3.8-flash-high"})
        json.dump(fo, open(os.path.join(S, "verslagcijfers", f"{fo['fund_id']}.json"), "w"), ensure_ascii=False, indent=1)
        print("  geschreven", fo["fund_id"], naam, flush=True)
    return "ok"


print(len(te_doen), "taken", flush=True)
with ThreadPoolExecutor(PARALLEL) as ex:
    for uitkomst in ex.map(draai, te_doen):
        if uitkomst == "quotum":
            print("quotum op — stop", flush=True)
            ex.shutdown(cancel_futures=True)
            break
