"""Knip per jaarverslag de pagina's met kerncijfers eruit, met paginanummer, voor de uitleesagents."""
import json, os, re, sqlite3, sys
import fitz

BASE = os.path.expanduser("~/pensioenfondsen-app")
UIT = sys.argv[1]
os.makedirs(UIT, exist_ok=True)
con = sqlite3.connect(os.path.join(BASE, "data/processed/pension_funds.db"))

TREF = {
    r"kerncijfers|meerjarenoverzicht|meerjaren ?overzicht|key figures": 6,
    r"beleidsdekkingsgraad": 3, r"actuele dekkingsgraad|nominale dekkingsgraad": 2,
    r"re[eë]le dekkingsgraad": 3, r"vereiste dekkingsgraad": 2,
    r"beleggingsrendement|totaal rendement|rendement op beleggingen": 2,
    r"belegd vermogen|beleggingen voor risico pensioenfonds|totaal beleggingen": 2,
    r"aantal deelnemers|actieve deelnemers|gewezen deelnemers|slapers|pensioengerechtigden": 2,
    r"renteafdekking|afdekking van het renterisico|rente-afdekking": 1,
    r"vastgesteld|ondertekening|vaststelling van het jaarverslag|datum van vaststelling": 1,
    r"invaren|invaardatum|transitiedatum": 1,
}

groepen = {}
for fid, naam, src in con.execute("select a.fund_id, f.name, a.source_pdf from fund_analysis a "
                                  "join funds f on f.id=a.fund_id where a.fiscal_year=2025"):
    groepen.setdefault(src, []).append((fid, naam))

kaart = json.load(open(sys.argv[2])) if len(sys.argv) > 2 and sys.argv[2] != "-" else {}
alleen = {int(x) for x in sys.argv[3].split(",")} if len(sys.argv) > 3 else None
index = []
for src, fondsen in sorted(groepen.items(), key=lambda kv: kv[1][0][0]):
    if alleen and not alleen & {f for f, _ in fondsen}:
        continue
    pad = src if src and not src.startswith("http") else None
    if not pad or not os.path.exists(os.path.join(BASE, pad)):
        eigen = [p for p in (f"data/annual_reports/{f}_" for f, _ in fondsen)]
        kandidaten = [os.path.join("data/annual_reports", n) for n in os.listdir(os.path.join(BASE, "data/annual_reports"))
                      if any(n.startswith(os.path.basename(e)) and n.endswith("_2025.pdf") for e in eigen)]
        if kandidaten:
            pad = kandidaten[0]
    if not pad:
        for doel, v in kaart.items():
            if set(v["ids"]) & {f for f, _ in fondsen} and v.get("url"):
                pad = doel
    if not pad or not os.path.exists(os.path.join(BASE, pad)):
        index.append({"bron": src, "fondsen": fondsen, "tekst": None, "reden": "pdf niet op schijf"})
        continue
    doc = fitz.open(os.path.join(BASE, pad))
    paginas = [p.get_text() for p in doc]
    doc.close()
    scores = []
    for i, t in enumerate(paginas):
        laag = t.lower()
        s = sum(w * min(len(re.findall(p, laag)), 3) for p, w in TREF.items())
        s += min(len(re.findall(r"\d+[,.]\d+ ?%", t)), 20) * 0.3
        scores.append((s, i))
    gekozen = sorted(i for s, i in sorted(scores, reverse=True)[:14] if s > 3)
    gekozen = sorted(set(gekozen) | set(range(min(3, len(paginas)))))
    naam_bestand = os.path.splitext(os.path.basename(pad))[0] + ".txt"
    with open(os.path.join(UIT, naam_bestand), "w") as f:
        f.write(f"# bron: {pad} ({len(paginas)} pagina's)\n# fondsen: {fondsen}\n\n")
        for i in gekozen:
            f.write(f"\n===== PDF-pagina {i + 1} =====\n{paginas[i]}\n")
    index.append({"bron": src, "pad": pad, "paginas": len(paginas), "fondsen": fondsen,
                  "tekst": os.path.join(UIT, naam_bestand)})

json.dump(index, open(os.path.join(UIT, "index_extra.json" if alleen else "index.json"), "w"), ensure_ascii=False, indent=1)
print(len(index), "verslagen;", sum(1 for x in index if x["tekst"]), "geknipt;",
      sum(1 for x in index if not x["tekst"]), "niet op schijf")
