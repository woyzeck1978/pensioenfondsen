"""Laat agy (Gemini) elke voorgestelde 2025-wijziging onafhankelijk nalezen tegen de verslagpagina's.

Items komen uit correctielog_2025.csv (vervangen, ter_beoordeling, gevuld). Per fonds worden alle
items samen met de betrokken pagina's één keer meegestuurd; een taak bevat een handvol fondsen.
Taken lopen parallel. Items met een uitkomst in tweede_lezing/uit_*.json worden overgeslagen,
zodat het script herstartbaar is.
"""
import csv, glob, json, os, subprocess, sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

S = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.expanduser("~/pensioenfondsen-app")
MAP = os.path.join(S, "tweede_lezing")
os.makedirs(MAP, exist_ok=True)
DELEGATE = "/Users/webkowuite/Library/Mobile Documents/com~apple~CloudDocs/R/.claude/skills/delegate/delegate.sh"
MAX_TEKENS = 90_000
PARALLEL = int(sys.argv[1]) if len(sys.argv) > 1 else 4

LABEL = {"beleggingsrendement_pct": "totaal beleggingsrendement in %", "beleidsdekkingsgraad_pct": "beleidsdekkingsgraad in %",
         "nominale_dekkingsgraad_pct": "actuele/nominale dekkingsgraad in %", "reele_dekkingsgraad_pct": "reële dekkingsgraad in %",
         "vereiste_dekkingsgraad_pct": "vereiste dekkingsgraad in %",
         "rente_afdekking_pct": "renteafdekking in % (gemeten stand ultimo jaar, niet het strategische doel)",
         "aum_euro_bn": "belegd vermogen in miljarden euro", "deelnemers_actief": "aantal actieve deelnemers",
         "deelnemers_slapers": "aantal gewezen deelnemers (slapers)", "deelnemers_pensioengerechtigd": "aantal pensioengerechtigden",
         "deelnemers_totaal": "totaal aantal deelnemers"}

gedaan = set()
for f in glob.glob(os.path.join(MAP, "uit_*.json")):
    try:
        gedaan.update(r["nr"] for r in json.load(open(f)).get("items", []))
    except Exception:
        pass

paden = {json.load(open(f))["fund_id"]: json.load(open(f))["pad"] for f in glob.glob(os.path.join(S, "verslagcijfers", "*.json"))}

per_fonds = defaultdict(list)
for r in csv.DictReader(open(os.path.join(S, "correctielog_2025.csv"))):
    if r["jaar"] != "2025" or r["actie"] not in ("vervangen", "ter_beoordeling", "gevuld") or not r["pagina"]:
        continue
    nr = f"{r['fund_id']}-{r['veld']}"
    if nr not in gedaan and int(r["fund_id"]) in paden:
        per_fonds[int(r["fund_id"])].append(r)


def pagina_tekst(pdf, paginas):
    delen = []
    for p in sorted(paginas):
        t = subprocess.run(["pdftotext", "-layout", "-f", str(p), "-l", str(p), pdf, "-"], capture_output=True, text=True).stdout
        delen.append(f"===== PDF-pagina {p} =====\n{t[:9000]}")
    return "\n".join(delen)


fondsblokken = []
for fid, items in per_fonds.items():
    pdf = os.path.join(BASE, paden[fid])
    paginas = {int(float(r["pagina"])) for r in items}
    tekst = pagina_tekst(pdf, paginas)
    regels = [f"\n## Fonds {fid}: {items[0]['fonds']}\n### Na te lezen\n"]
    for r in items:
        regels.append(f"- nr `{fid}-{r['veld']}` · begrip: {LABEL[r['veld']]} · database: {r['database'] or 'leeg'} · "
                      f"voorstel: {r['verslag']} (pagina {int(float(r['pagina']))})\n")
    regels.append(f"### Verslagtekst\n```\n{tekst}\n```\n")
    fondsblokken.append(("".join(regels), len(items)))

taken, huidig, lengte = [], [], 0
for blok, n in fondsblokken:
    if huidig and lengte + len(blok) > MAX_TEKENS:
        taken.append(huidig)
        huidig, lengte = [], 0
    huidig.append((blok, n))
    lengte += len(blok)
if huidig:
    taken.append(huidig)
print(sum(n for _, n in fondsblokken), "items in", len(fondsblokken), "fondsen,", len(taken), "taken", flush=True)

SCHEMA = os.path.join(MAP, "schema.json")
json.dump({"type": "object", "additionalProperties": False, "required": ["items"], "properties": {"items": {
    "type": "array", "items": {"type": "object", "additionalProperties": False,
                               "required": ["nr", "oordeel", "waarde_2025", "citaat"],
                               "properties": {"nr": {"type": "string"},
                                              "oordeel": {"type": "string", "enum": ["voorstel_klopt", "database_klopt", "beide_fout", "niet_te_bepalen"]},
                                              "waarde_2025": {"type": ["number", "null"]},
                                              "citaat": {"type": "string"}}}}}}, open(SCHEMA, "w"))

KOP = ("# Opdracht\nJe controleert cijfers uit Nederlandse pensioenfondsjaarverslagen over boekjaar 2025. Per fonds krijg je "
       "een lijst na te lezen items (begrip, waarde die nu in een database staat, voorgestelde waarde) en de letterlijke tekst "
       "van de betrokken verslagpagina's (pdftotext; tabelkolommen kunnen verschoven staan).\n\n"
       "Bepaal per item uit de tekst zelf wat de waarde over 2025 is. Kerncijfertabellen tonen meerdere jaren naast elkaar: lees "
       "de kolomkop. Bedragen kunnen in duizenden of miljoenen staan: reken om naar de gevraagde eenheid. Bij renteafdekking telt "
       "de gemeten stand, niet een strategisch doel of beleidspercentage.\n\n"
       "Geef per item: `oordeel` = voorstel_klopt | database_klopt | beide_fout | niet_te_bepalen; `waarde_2025` = de waarde die "
       "jij in de tekst leest (null als niet te bepalen); `citaat` = korte letterlijke tekst waarop je oordeel rust. Verzin niets. "
       "Gebruik geen tools: alles wat je nodig hebt staat hieronder.\n\n"
       "# Uitvoer\nJSON volgens het schema: {\"items\": [...]}, precies één object per item, met exact dezelfde `nr`.\n\n# Fondsen\n")


RUN = __import__("time").strftime("%Y%m%d-%H%M%S")


def draai(i):
    # Elke run eigen bestandsnamen: een herstart overschreef eerder de uitkomsten van de vorige run.
    taak = os.path.join(MAP, f"taak_{RUN}_{i:03d}.md")
    uit = os.path.join(MAP, f"uit_{RUN}_{i:03d}.json")
    with open(taak, "w") as f:
        f.write(KOP + "".join(b for b, _ in taken[i]))
    r = subprocess.run(["bash", DELEGATE, "--model", "gemini-3.8-flash-high", "--out", uit, "--schema", SCHEMA,
                        "--timeout", "12m", taak], capture_output=True, text=True)
    n_in = sum(n for _, n in taken[i])
    try:
        n_uit = len(json.load(open(uit))["items"])
    except Exception:
        n_uit = 0
    print(f"taak {i}: exit {r.returncode}, {n_uit}/{n_in} items", flush=True)
    return r.returncode


with ThreadPoolExecutor(PARALLEL) as pool:
    for code in pool.map(draai, range(len(taken))):
        if code == 3:
            print("quotum op; resterende taken lopen nog af, daarna herstarten na reset", flush=True)
