"""LLM-gegenereerde analyse van een jaarverslag-PDF.

Voor elke fund × fiscal_year wordt een gestructureerde JSON gevraagd
aan de lokale Ollama (mistral-small):

  {"summary":           "<3-5 zin samenvatting>",
   "highlights":        ["...", "...", "..."],   # 3-5 positieve punten
   "lowlights":         ["...", "...", "..."],   # 3-5 aandachtspunten
   "key_risks":         ["...", "...", "..."],
   "transitie_status":  "<1 zin over WTP-status>"}

Bron-pagina's worden gefilterd op headers die meestal de relevante
narrative bevatten: 'Voorwoord', 'Bestuursverslag', 'Samenvatting',
'Belangrijkste ontwikkelingen', 'Strategie en beleid', 'Risico'.

Output naar tabel fund_analysis (PRIMARY KEY fund_id + fiscal_year).
Re-run zonder --force is een no-op voor reeds verwerkte rijen.

Usage:
  python3 llm_extract_analysis.py --funds 13,111,32     # 3 FY2025 PDFs
  python3 llm_extract_analysis.py --top 30
  python3 llm_extract_analysis.py --funds 111 --force   # overschrijf
  python3 llm_extract_analysis.py --funds 91 --pages 6-10 --force

Twee dingen om te weten voor je opnieuw genereert:

1. --fiscal-year is een VOORKEUR voor welke PDF gekozen wordt, geen filter op
   welke rij wordt weggeschreven. Heeft een fonds geen PDF van dat jaar, dan pakt
   het script de nieuwste PDF en schrijft de analyse weg onder HET JAAR VAN DIE
   PDF. Een run met --fiscal-year 2025 over 24 fondsen leverde zo 6 FY2025-rijen
   op en 4 overschreven FY2024-rijen. Controleer na een batch dus of de rijen
   onder het bedoelde boekjaar staan.

   Het boekjaar komt uit de bestandsnaam, en staat dat er niet in (`73_Ahold_
   Delhaize.pdf` — dat geldt voor de meeste PDF's) dan wordt het van de omslag
   gelezen. Lukt ook dat niet, dan wordt het fonds overgeslagen in plaats van
   onder FY0 weggeschreven; met --fiscal-year dwing je het jaar dan zelf af.

2. De paginakeuze is verbeterd (nalevingsbijlagen worden overgeslagen, een
   trefwoord telt hooguit één keer, en een pagina erft het hoofdstuksignaal van
   maximaal twee pagina's terug). Dat haalde bij PDN het voorwoord binnen in
   plaats van de compliance-checklist. Maar het is een defectreparatie, geen
   garantie op betere teksten: bij een steekproef van vijf fondsen werd de
   samenvatting soms juist vager. Regenereer dus per fonds en kijk naar de
   uitkomst; wijkt de automatische keuze af, gebruik dan --pages.
"""

import argparse
import glob
import json
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime

import fitz

OLLAMA = "http://100.71.107.24:11434"
MODEL = "mistral-small"
DB_PATH = "../../data/processed/pension_funds.db"
DIRS = ["../../data/annual_reports", "../../data/reports"]
LOG_DIR = "../../logs/llm_extract"

# Page selection: prefer narrative pages (voorwoord/bestuursverslag) over
# pure-numbers pages (kerncijfers). Score = header matches + sentence count.
HEADER_PATTERNS = [
    re.compile(r"\bvoorwoord\b", re.I),
    re.compile(r"\bbestuursverslag\b", re.I),
    re.compile(r"\bsamenvatting\b", re.I),
    re.compile(r"\bbelangrijkste\s+(?:ontwikkelingen|gebeurtenissen|punten)\b", re.I),
    re.compile(r"\bstrategie\s+en\s+beleid\b", re.I),
    re.compile(r"\brisicobeheersing\b|\brisicoparagraaf\b", re.I),
    re.compile(r"\bbericht\s+van\s+(?:het\s+)?bestuur\b", re.I),
    re.compile(r"\bvooruitblik\b|\bvooruitzicht\b", re.I),
]


# Filenames containing these tokens are NOT jaarverslagen and must be skipped:
# transition plans, ESG/sustainability reports, SFDR disclosures, infographics.
_NON_JAARVERSLAG = re.compile(
    r"(transitieplan|_esg|_sfdr|infographic|beleggingsmix|samenvatting)",
    re.I,
)


def build_inventory() -> dict[int, list[tuple[int, str]]]:
    by_fund = defaultdict(list)
    for d in DIRS:
        for path in glob.glob(f"{d}/*.pdf"):
            base = os.path.basename(path)
            if _NON_JAARVERSLAG.search(base):
                continue
            m = re.match(r"^(\d+)[_\s]", base)
            if not m:
                continue
            ym = re.search(r"(20\d{2})", base)
            by_fund[int(m.group(1))].append((int(ym.group(1)) if ym else 0, path))
    return by_fund


# Het boekjaar staat maar bij een handjevol PDF's in de bestandsnaam; de meeste
# heten `73_Ahold_Delhaize.pdf`. Die belandden op fiscal_year = 0. Op de omslag of
# de titelpagina staat het jaar vrijwel altijd wel, gekoppeld aan een verslag-woord.
_VERSLAG = (r"(?:jaarverslag|jaarbericht|jaarrapport|jaarrekening|verslagjaar|"
            r"bestuursverslag|annual\s+report)")
JAAR_IN_TEKST = [
    re.compile(rf"\b{_VERSLAG}\b.{{0,30}}?\b(20\d{{2}})\b", re.I),
    re.compile(rf"\b(20\d{{2}})\b.{{0,20}}?\b{_VERSLAG}\b", re.I),
    re.compile(r"\bboekjaar\b.{0,30}?\b(20\d{2})\b", re.I),
    re.compile(r"\bover\s+het\s+(?:boek)?jaar\b.{0,10}?\b(20\d{2})\b", re.I),
]
JAAR_MIN = 2015
JAAR_MAX = datetime.now().year

_boekjaar_cache: dict[str, int] = {}


def boekjaar_uit_inhoud(pdf_path: str, max_pages: int = 4) -> int:
    """Boekjaar van de omslag/titelpagina lezen; 0 als er niets bruikbaars staat.

    Alleen jaartallen die vlak bij een verslag-woord staan tellen mee, anders pikt
    het regelnummers en bedragen op. Van de treffers wint de vroegste, niet de
    hoogste: het boekjaar staat op de omslag, en wat daarna komt is de datum van
    ondertekening ('Rijswijk, 13 juni 2025') of een vooruitblik op het jaar erna.

    De witruimte wordt eerst platgeslagen, want op een omslag staan 'Jaarverslag'
    en het jaartal vrijwel altijd op aparte regels.
    """
    if pdf_path in _boekjaar_cache:
        return _boekjaar_cache[pdf_path]
    jaar = 0
    try:
        doc = fitz.open(pdf_path)
        tekst = " ".join(doc[i].get_text() for i in range(min(max_pages, len(doc))))
        doc.close()
        tekst = re.sub(r"\s+", " ", tekst)
        treffers = [
            (m.start(), int(m.group(1)))
            for p in JAAR_IN_TEKST for m in p.finditer(tekst)
            if JAAR_MIN <= int(m.group(1)) <= JAAR_MAX
        ]
        if treffers:
            jaar = min(treffers)[1]
    except Exception:
        jaar = 0
    _boekjaar_cache[pdf_path] = jaar
    return jaar


def parse_pages(spec: str) -> list[int]:
    """'6-10' of '6,7,8' -> [6,7,8,9,10]. Voor als de automatische paginakeuze
    ernaast zit en je zelf wilt aanwijzen welk hoofdstuk het narratief bevat."""
    uit: list[int] = []
    for deel in spec.split(","):
        deel = deel.strip()
        if not deel:
            continue
        if "-" in deel:
            a, b = deel.split("-", 1)
            uit.extend(range(int(a), int(b) + 1))
        else:
            uit.append(int(deel))
    return sorted(set(uit))


def pages_as_context(pdf_path: str, pages: list[int]) -> tuple[str, list[int], int]:
    doc = fitz.open(pdf_path)
    n_pages = len(doc)
    geldig = [p for p in pages if 1 <= p <= n_pages]
    ctx = "\n\n--- BREAK ---\n\n".join(
        f"[page {p}]\n{doc[p - 1].get_text().strip()[:2400]}" for p in geldig
    )
    doc.close()
    return ctx, geldig, n_pages


def pick_pdf_for_year(items, prefer_year: int | None):
    """Return (year, path). If prefer_year given and present, use that. Else newest.

    Staat er geen jaartal in de bestandsnaam, dan wordt het uit de omslag gelezen;
    dat scheelt een rij op fiscal_year 0.
    """
    opgelost = [(y or boekjaar_uit_inhoud(p), p) for y, p in items]
    if prefer_year is not None:
        for y, p in opgelost:
            if y == prefer_year:
                return y, p
    return sorted(opgelost, reverse=True)[0]


def _is_toc_page(text: str) -> bool:
    # TOC = many lines that are bare page numbers
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    if len(lines) < 8:
        return False
    bare_nums = sum(1 for ln in lines if re.fullmatch(r"\d{1,3}", ln))
    # Ook de variant met stippellijnen: "Voorwoord .......... 6"
    leaders = sum(1 for ln in lines if re.search(r"\.{4,}\s*\d{1,3}$", ln))
    return (bare_nums + leaders) / len(lines) > 0.30


# Bijlagen die vol staan met dezelfde trefwoorden als het bestuursverslag, maar
# geen narratief bevatten: de nalevingstabellen achterin het jaarverslag.
RE_BIJLAGE = re.compile(
    r"checklist\s+code\s+pensioenfondsen|normen\s+uit\s+de\s+code|"
    r"naleving\s+code\s+pensioenfondsen",
    re.I,
)
KOP_ZONE = 200  # tekens vanaf de bovenkant die als paginakop gelden


def _pagina_scores(text: str) -> tuple[int, int, int]:
    """(kop, vermelding, zinnen). Elk patroon telt hooguit één keer mee.

    Tellen hoe váák een trefwoord voorkomt werkt averechts: een nalevingstabel
    die 'bestuursverslag' acht keer noemt wint het dan van het hoofdstuk dat
    zichzelf één keer zo noemt. Wat telt is of het woord er staat, en vooral of
    het bovenaan de pagina staat — dan is het een hoofdstuktitel.
    """
    kop_tekst = text[:KOP_ZONE]
    kop = sum(1 for p in HEADER_PATTERNS if p.search(kop_tekst))
    vermelding = sum(1 for p in HEADER_PATTERNS if p.search(text))
    zinnen = min(5, len(re.findall(r"\.\s+[A-Z]", text)))
    return kop, vermelding, zinnen


def find_narrative_pages(pdf_path: str, max_pages: int = 5) -> tuple[str, list[int], int]:
    doc = fitz.open(pdf_path)
    kandidaten = []  # (paginanummer, kop, vermelding, zinnen, tekst)
    koppen: set[int] = set()
    for i, page in enumerate(doc):
        if i >= 200:
            break
        text = page.get_text()
        if _is_toc_page(text) or RE_BIJLAGE.search(text[:400]):
            continue
        kop, vermelding, zinnen = _pagina_scores(text)
        # Titelpagina's bevatten vaak nauwelijks lopende tekst. Ze zijn zelf geen
        # goede context, maar markeren wel waar een hoofdstuk begint -- dus
        # noteren we ze los van het zinnenfilter, anders krijgt de pagina erna
        # nooit zijn vervolgbonus.
        if kop:
            koppen.add(i + 1)
        if zinnen < 2:
            continue
        kandidaten.append((i + 1, kop, vermelding, zinnen, text))
    n_pages = len(doc)
    doc.close()
    if not kandidaten:
        return "", [], n_pages

    # Een pagina hoort bij het hoofdstuk dat hooguit twee pagina's eerder begon.
    # Dat signaal moet even zwaar wegen als een kop op de pagina zelf: de
    # titelpagina bevat meestal alleen de titel, het verhaal staat erna.
    scored = []
    for pagina, kop, vermelding, zinnen, text in kandidaten:
        hoort_bij_hoofdstuk = kop or (pagina - 1) in koppen or (pagina - 2) in koppen
        score = (20 if hoort_bij_hoofdstuk else 0) + vermelding * 3 + zinnen
        scored.append((score, pagina, text))

    scored.sort(key=lambda x: (-x[0], x[1]))
    chosen = sorted(scored[:max_pages], key=lambda x: x[1])  # weer op paginavolgorde
    pages = [c[1] for c in chosen]
    ctx = "\n\n--- BREAK ---\n\n".join(
        f"[page {n}]\n{t.strip()[:2400]}" for _, n, t in chosen
    )
    return ctx, pages, n_pages


PROMPT = """Je krijgt fragmenten uit een Nederlands pensioenfonds-jaarverslag \
(voorwoord, bestuursverslag, samenvatting). Geef ALLEEN JSON terug \
— geen toelichting, geen markdown.

Schema (alle velden in het Nederlands):
{{
  "summary":           "<3-5 zinnen, max 80 woorden, hoofdpunten van het jaar>",
  "highlights":        ["<bullet 1>", "<bullet 2>", "<bullet 3>", "<eventueel 4>", "<eventueel 5>"],
  "lowlights":         ["<bullet 1>", "<bullet 2>", "<eventueel 3>"],
  "key_risks":         ["<bullet 1>", "<bullet 2>", "<eventueel 3>"],
  "transitie_status":  "<1-2 zinnen over de Wtp-transitie / invaren-status>"
}}

Regels:
- Schrijf neutraal, feitelijk. Geen marketing-taal.
- Maximaal 25 woorden per bullet.
- highlights = positieve / behaalde resultaten.
- lowlights = teleurstellende ontwikkelingen of zorgpunten.
- key_risks = financiële, operationele of regelgevingsrisico's die het verslag noemt.
- transitie_status: vermeld of het fonds is ingevaren, een datum heeft, of nog beslist.
- Als een sectie geen materiaal heeft, geef een lege array [].
- GEEN andere velden dan in schema.

BRON:
{context}
"""


def call_ollama(prompt: str, timeout: int = 600) -> tuple[dict, float]:
    body = json.dumps({
        "model": MODEL, "prompt": prompt, "stream": False, "format": "json",
        "options": {"num_ctx": 16000, "temperature": 0.2, "num_predict": 700},
    }).encode()
    req = urllib.request.Request(
        f"{OLLAMA}/api/generate", data=body,
        headers={"Content-Type": "application/json"},
    )
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        resp = json.load(r)
    return resp, time.time() - t0


def sanitize(d: dict) -> dict:
    """Normalize types & guard against junk."""
    out = {"summary": None, "highlights": [], "lowlights": [], "key_risks": [], "transitie_status": None}
    if not isinstance(d, dict):
        return out
    s = d.get("summary")
    if isinstance(s, str) and s.strip():
        out["summary"] = s.strip()[:800]
    for k in ("highlights", "lowlights", "key_risks"):
        v = d.get(k)
        if isinstance(v, list):
            cleaned = [str(x).strip()[:300] for x in v if isinstance(x, (str, int, float)) and str(x).strip()]
            out[k] = cleaned[:5]
    t = d.get("transitie_status")
    if isinstance(t, str) and t.strip():
        out["transitie_status"] = t.strip()[:400]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--funds", type=str, default="", help="comma-separated fund ids")
    ap.add_argument("--top", type=int, default=0, help="top-N by AUM")
    ap.add_argument("--fiscal-year", type=int, default=None,
                    help="prefer this fiscal year (default: newest PDF)")
    ap.add_argument("--pages", type=str, default="",
                    help="paginas handmatig aanwijzen, bv. 6-10 (alleen zinvol bij 1 fonds)")
    ap.add_argument("--force", action="store_true",
                    help="overwrite existing fund_analysis rows")
    args = ap.parse_args()

    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f"analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    log_f = open(log_path, "a", buffering=1)
    def log(m):
        line = f"[{datetime.now().strftime('%H:%M:%S')}] {m}"
        print(line, flush=True)
        log_f.write(line + "\n")

    inv = build_inventory()
    con = sqlite3.connect(DB_PATH)

    if args.funds:
        targets = [int(x) for x in args.funds.split(",") if x.strip()]
    elif args.top:
        rows = con.execute("""
            SELECT id FROM funds WHERE aum_euro_bn IS NOT NULL
            ORDER BY aum_euro_bn DESC LIMIT ?
        """, (args.top,)).fetchall()
        targets = [r[0] for r in rows]
    else:
        targets = sorted(inv.keys())

    targets = [t for t in targets if t in inv]
    if not args.force:
        done = {(r[0], r[1]) for r in con.execute(
            "SELECT fund_id, fiscal_year FROM fund_analysis"
        ).fetchall()}
    else:
        done = set()

    n = len(targets)
    log(f"start | {n} fund(s) | year={args.fiscal_year or 'newest'} | force={args.force}")
    t0 = time.time()
    hits = skips = errs = 0

    for i, fid in enumerate(targets, 1):
        try:
            year, pdf = pick_pdf_for_year(inv[fid], args.fiscal_year)
            if not year:
                # Liever geen rij dan een rij op FY0: die is op het dashboard niet
                # van een echt boekjaar te onderscheiden en moet met de hand weg.
                # Wie het toch wil, geeft het jaar mee met --fiscal-year.
                if args.fiscal_year:
                    year = args.fiscal_year
                else:
                    log(f"[{i}/{n}] fid={fid} {os.path.basename(pdf)[:50]} "
                        f"SKIP geen boekjaar herkend (geef --fiscal-year mee)")
                    skips += 1
                    continue
            if not args.force and (fid, year) in done:
                log(f"[{i}/{n}] fid={fid} {year} already done — skip")
                skips += 1
                continue
            if args.pages:
                ctx, pages, n_pdf = pages_as_context(pdf, parse_pages(args.pages))
            else:
                ctx, pages, n_pdf = find_narrative_pages(pdf)
            if not ctx:
                log(f"[{i}/{n}] fid={fid} {year} {os.path.basename(pdf)[:50]} SKIP no narrative pages")
                skips += 1
                continue
            resp, dt = call_ollama(PROMPT.format(context=ctx))
            try:
                raw = json.loads(resp.get("response", ""))
            except Exception as e:
                log(f"[{i}/{n}] fid={fid} JSON parse failed: {e!r}")
                errs += 1
                continue
            clean = sanitize(raw)
            con.execute("""
                INSERT INTO fund_analysis
                    (fund_id, fiscal_year, summary, highlights_json,
                     lowlights_json, key_risks_json, transitie_status,
                     source_pdf, generated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(fund_id, fiscal_year) DO UPDATE SET
                    summary=excluded.summary,
                    highlights_json=excluded.highlights_json,
                    lowlights_json=excluded.lowlights_json,
                    key_risks_json=excluded.key_risks_json,
                    transitie_status=excluded.transitie_status,
                    source_pdf=excluded.source_pdf,
                    generated_at=CURRENT_TIMESTAMP
            """, (fid, year, clean["summary"],
                  json.dumps(clean["highlights"], ensure_ascii=False),
                  json.dumps(clean["lowlights"], ensure_ascii=False),
                  json.dumps(clean["key_risks"], ensure_ascii=False),
                  clean["transitie_status"], os.path.relpath(pdf, "../..")))
            con.commit()
            hits += 1
            log(f"[{i}/{n}] fid={fid} FY{year} {dt:>4.0f}s  "
                f"sum={'Y' if clean['summary'] else 'N'} "
                f"hl={len(clean['highlights'])} ll={len(clean['lowlights'])} "
                f"risks={len(clean['key_risks'])}")
        except urllib.error.URLError as e:
            log(f"[{i}/{n}] fid={fid} URL-ERR: {e}")
            errs += 1
        except Exception as e:
            log(f"[{i}/{n}] fid={fid} ERR: {e}")
            errs += 1

    log(f"done | {time.time()-t0:.0f}s | hits={hits} skips={skips} errors={errs}")
    log_f.close()
    con.close()


if __name__ == "__main__":
    main()
