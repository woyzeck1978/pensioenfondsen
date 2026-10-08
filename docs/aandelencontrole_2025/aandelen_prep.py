"""Knip per jaarverslag 2025 de pagina's over de inrichting van de aandelenstrategie eruit, met paginanummer, voor de uitleesagents.

Gebruikt de verslagpaden uit docs/verslagcontrole_2025/verslagcijfers/<id>.json (173 fondsen).
Aanroep: aandelen_prep.py <uitvoermap> [fund_ids,komma-gescheiden]
"""
import glob, json, os, re, subprocess, sys

BASE = os.path.expanduser("~/pensioenfondsen-app")
KERN = os.path.join(BASE, "docs/verslagcontrole_2025/verslagcijfers")
UIT = sys.argv[1]
os.makedirs(UIT, exist_ok=True)
alleen = {int(x) for x in sys.argv[2].split(",")} if len(sys.argv) > 2 else None

TREF = {
    r"\baandelen\b": 2,
    r"beleggingsmix|strategische (beleggings)?mix|normportefeuille|strategische allocatie|strategisch gewicht|strategische portefeuille": 3,
    r"verdeling (van de )?beleggingen|samenstelling (van de )?beleggingen|beleggingscategorie": 2,
    r"private equity|beursgenoteerde aandelen": 2,
    r"vermogensbeheerder|fiduciair|uitbesteed|uitbesteding|mandaat|mandaten|beheerder": 3,
    r"blackrock|northern trust|state street|robeco|apg|pggm|\bmn\b|achmea investment|kempen|van lanschot|goldman sachs|aegon asset|cardano|amundi|vanguard|dws|ubs|axa|legal & general|lgim|columbia|russell|nn investment|nn ip|bnp paribas|schroders|baillie|wellington|dimensional|tkp|a\.s\.r\.|asr vermogensbeheer|invesco|fidelity|t\. rowe|jp ?morgan|lazard|insight|pimco": 4,
    r"kosten vermogensbeheer|vermogensbeheerkosten|beheerkosten|transactiekosten|prestatievergoeding|performance ?fee|basispunten": 3,
    r"passief|actief beheer|actieve? beheer|indexbelegg|indexfonds|index ?tracking|tracking error|enhanced|factor(beleggen|premie|strateg)|smart beta|small ?cap": 3,
    r"benchmark|msci|ftse|stoxx|s&p": 2,
    r"uitsluiting|uitgesloten|engagement|stembeleid|best.in.class|co2|co₂|klimaat|parijs|paris.aligned|duurzaam|maatschappelijk verantwoord|esg|sfdr|impact": 2,
    r"noord.amerika|verenigde staten|europa|pacific|azi[eë]|opkomende (markten|landen)|ontwikkelde (markten|landen)|emerging|regio": 2,
    r"valuta(risico|afdekking|-afdekking)|afdekking van (het )?valuta": 1,
    r"matchingportefeuille|returnportefeuille|rendementsportefeuille|overrendement": 1,
}
BALANS = re.compile(r"^\s*(-\s*|•\s*|totaal beleggingen in\s*)?aandelen\b[^\n]{0,40}?\d{1,3}(\.\d{3})+", re.I | re.M)
PCT = re.compile(r"\d+[,.]\d+ ?%")

paden = {}
for f in glob.glob(os.path.join(KERN, "*.json")):
    d = json.load(open(f))
    paden[d["fund_id"]] = (d["naam"], d["pad"])

# meerdere fondsen (kringen) delen soms één verslag: groepeer op pad
groepen = {}
for fid, (naam, pad) in sorted(paden.items()):
    if alleen and fid not in alleen:
        continue
    groepen.setdefault(pad, []).append((fid, naam))

index = []
for pad, fondsen in sorted(groepen.items(), key=lambda kv: kv[1][0][0]):
    vol = os.path.join(BASE, pad)
    if not os.path.exists(vol):
        index.append({"pad": pad, "fondsen": fondsen, "tekst": None, "reden": "pdf niet op schijf"})
        continue
    tekst = subprocess.run(["pdftotext", "-layout", vol, "-"], capture_output=True, text=True).stdout
    paginas = tekst.split("\f")
    if paginas and not paginas[-1].strip():
        paginas.pop()
    scores = []
    for i, t in enumerate(paginas):
        laag = t.lower()
        s = sum(w * min(len(re.findall(p, laag)), 3) for p, w in TREF.items())
        s += 8 * min(len(BALANS.findall(t)), 2)
        s += min(len(PCT.findall(t)), 20) * 0.3
        scores.append((s, i))
    gekozen = sorted(i for s, i in sorted(scores, reverse=True)[:18] if s > 8)
    naam_bestand = os.path.splitext(os.path.basename(pad))[0] + ".txt"
    with open(os.path.join(UIT, naam_bestand), "w") as f:
        f.write(f"# bron: {pad} ({len(paginas)} pagina's)\n# fondsen: {fondsen}\n\n")
        for i in gekozen:
            f.write(f"\n===== PDF-pagina {i + 1} =====\n{paginas[i][:9000]}\n")
    index.append({"pad": pad, "paginas": len(paginas), "gekozen": [i + 1 for i in gekozen], "fondsen": fondsen,
                  "tekst": os.path.join(UIT, naam_bestand)})

json.dump(index, open(os.path.join(UIT, "index_extra.json" if alleen else "index.json"), "w"), ensure_ascii=False, indent=1)
print(len(index), "verslagen;", sum(1 for x in index if x["tekst"]), "geknipt;",
      sum(1 for x in index if not x["tekst"]), "niet op schijf")
