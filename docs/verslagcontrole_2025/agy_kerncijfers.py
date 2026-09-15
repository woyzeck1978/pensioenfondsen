"""Laat agy de kerncijfers uit geknipte verslagtekst lezen en schrijf verslagcijfers/<id>.json.

Invoer: een index-bestand (lijst met tekst, pad, fondsen) uit kerncijfers_prep.py.
Fondsen die al een verslagcijfers-bestand hebben worden overgeslagen.
"""
import json, os, subprocess, sys

S = os.path.dirname(os.path.abspath(__file__))
DELEGATE = "/Users/webkowuite/Library/Mobile Documents/com~apple~CloudDocs/R/.claude/skills/delegate/delegate.sh"
MAP = os.path.join(S, "agy_kern")
os.makedirs(MAP, exist_ok=True)
VELDEN = ["beleggingsrendement_pct", "beleidsdekkingsgraad_pct", "nominale_dekkingsgraad_pct", "reele_dekkingsgraad_pct",
          "vereiste_dekkingsgraad_pct", "aum_euro_bn", "deelnemers_actief", "deelnemers_slapers",
          "deelnemers_pensioengerechtigd", "deelnemers_totaal", "rente_afdekking_pct"]
getal = {"type": ["number", "null"]}
veld = {"type": "object", "additionalProperties": False, "required": ["2025", "2024", "pagina", "letterlijk"],
        "properties": {"2025": getal, "2024": getal, "pagina": {"type": ["integer", "null"]}, "letterlijk": {"type": "string"}}}
SCHEMA = {"type": "object", "additionalProperties": False, "required": ["fondsen"], "properties": {"fondsen": {"type": "array", "items": {
    "type": "object", "additionalProperties": False,
    "required": ["fund_id", "velden", "vaststellingsdatum", "invaardatum", "opmerkingen"],
    "properties": {
        "fund_id": {"type": "integer"},
        "velden": {"type": "object", "additionalProperties": False, "required": VELDEN, "properties": {v: veld for v in VELDEN}},
        "vaststellingsdatum": {"type": "object", "additionalProperties": False, "required": ["datum", "pagina"],
                               "properties": {"datum": {"type": ["string", "null"]}, "pagina": {"type": ["integer", "null"]}}},
        "invaardatum": {"type": "object", "additionalProperties": False, "required": ["datum", "status", "pagina"],
                        "properties": {"datum": {"type": ["string", "null"]},
                                       "status": {"type": "string", "enum": ["ingevaren", "gepland", "geen datum genoemd", "vaart niet in"]},
                                       "pagina": {"type": ["integer", "null"]}}},
        "opmerkingen": {"type": "string"}}}}}}
schema_pad = os.path.join(MAP, "schema.json")
json.dump(SCHEMA, open(schema_pad, "w"))

OPDRACHT = """# Opdracht
Je leest kerncijfers over boekjaar 2025 (en ter controle 2024) uit Nederlandse pensioenfondsjaarverslagen. Van elk verslag
krijg je een uitsnede van de relevante pagina's, gemarkeerd als "===== PDF-pagina N =====". Geef per fund_id één object.

Velden (null als het verslag het niet noemt):
- beleggingsrendement_pct: totaal rendement op beleggingen in % (inclusief renteafdekking)
- beleidsdekkingsgraad_pct, nominale_dekkingsgraad_pct (actuele dekkingsgraad), reele_dekkingsgraad_pct (alleen als expliciet genoemd), vereiste_dekkingsgraad_pct: ultimo jaar, in %
- aum_euro_bn: belegd vermogen in MILJARDEN euro (verslagen rapporteren in duizenden of miljoenen: reken om, noem de eenheid in `letterlijk`)
- deelnemers_actief, deelnemers_slapers (gewezen deelnemers), deelnemers_pensioengerechtigd, deelnemers_totaal: aantallen personen
- rente_afdekking_pct: GEMETEN afdekking van het renterisico ultimo jaar in %; een strategisch doel of beleidspercentage telt niet (dan null, en noem het in opmerkingen)
- vaststellingsdatum: de datum waarop het bestuur het jaarverslag vaststelde of ondertekende (YYYY-MM-DD)
- invaardatum: datum en status van de overgang naar de Wet toekomst pensioenen

Regels: kerncijfertabellen tonen meerdere jaren naast elkaar, LEES DE KOLOMKOP en neem de kolom 2025. `pagina` = het
PDF-paginanummer uit de markering. `letterlijk` = korte letterlijke tekst waar het 2025-getal staat (leeg als null).
Getallen met punt als decimaalteken (131,5% -> 131.5; 12.345 -> 12345). Verzin niets, schat niets.

# Uitvoer
JSON volgens het schema: {"fondsen": [ ... ]}.

# Verslagen
"""

index = json.load(open(sys.argv[1]))
te_doen = [x for x in index if x.get("tekst") and not all(
    os.path.exists(os.path.join(S, "verslagcijfers", f"{f}.json")) for f, _ in x["fondsen"])]
PER = int(sys.argv[2]) if len(sys.argv) > 2 else 2
for i in range(0, len(te_doen), PER):
    blok = te_doen[i:i + PER]
    taak = os.path.join(MAP, f"taak_{blok[0]['fondsen'][0][0]}.md")
    uit = os.path.join(MAP, f"uit_{blok[0]['fondsen'][0][0]}.json")
    with open(taak, "w") as f:
        f.write(OPDRACHT)
        for x in blok:
            f.write(f"\n## Verslag voor fund_id(s): {', '.join(f'{fid} ({n})' for fid, n in x['fondsen'])}\n```\n")
            f.write(open(x["tekst"]).read())
            f.write("\n```\n")
    r = subprocess.run(["bash", DELEGATE, "--model", "gemini-3.8-flash-high", "--out", uit, "--schema", schema_pad,
                        "--timeout", "10m", taak], capture_output=True, text=True)
    print(f"taak {os.path.basename(taak)}: exit {r.returncode} {r.stdout.strip()[-150:]} {r.stderr.strip()[-150:]}", flush=True)
    if r.returncode == 3:
        print("quotum op", flush=True)
        break
    if r.returncode not in (0, 4) or not os.path.exists(uit):
        continue
    try:
        res = json.load(open(uit))
    except Exception as e:
        print("  onleesbare uitvoer:", e, flush=True)
        continue
    toegestaan = {fid: (n, x["pad"]) for x in blok for fid, n in x["fondsen"]}
    for fo in res.get("fondsen", []):
        if fo["fund_id"] not in toegestaan:
            print("  onbekend fund_id genegeerd:", fo["fund_id"], flush=True)
            continue
        naam, pad = toegestaan[fo["fund_id"]]
        fo.update({"naam": naam, "pad": pad, "bron_model": "agy gemini-3.8-flash-high"})
        json.dump(fo, open(os.path.join(S, "verslagcijfers", f"{fo['fund_id']}.json"), "w"), ensure_ascii=False, indent=1)
        print("  geschreven", fo["fund_id"], naam, flush=True)
