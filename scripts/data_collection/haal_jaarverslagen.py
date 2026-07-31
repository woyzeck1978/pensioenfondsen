"""Haal jaarverslagen op voor fondsen waarvan de analyse achterloopt.

Zoekt in scraped_documents naar een jaarverslag-URL van het gevraagde boekjaar,
haalt die op en keurt het resultaat vóór opslag. Dat laatste is de kern: eerder
belandden een 403-pagina van 206 bytes, het jaarverslag van het Nederlands
Filmfonds en het toezichtverslag van een tbs-kliniek ongemerkt in data/, omdat
de downloader alleen keek of er íets terugkwam.

Elke download moet door vier controles:
  1. HTTP 200 en een %PDF-header
  2. groter dan MIN_BYTES en meer dan één pagina
  3. het boekjaar staat op de omslag
  4. een kenmerkend woord uit de fondsnaam komt in de eerste pagina's voor,
     tenzij de PDF van het eigen domein van het fonds komt -- dat is een sterker
     bewijs van herkomst. Het verslag van SPMS heet 'Uw pensioen. Ons
     specialisme' en noemt de fondsnaam nergens vooraan, maar staat op spms.nl.

Zakt een bestand daarop af, dan wordt het niet opgeslagen maar gemeld.

Staat er geen URL in scraped_documents, dan wordt met --via-site de eigen
documentenpagina van het fonds doorlopen. Dat is nodig omdat de scrape vooral
nieuwsberichten catalogiseert: bij 16 van 22 fondsen ontbrak de 2025-link
terwijl het verslag gewoon online stond. Die route gaat via een echte browser,
en haalt de PDF op met fetch() binnen de pagina — daarmee komt hij ook langs de
botblokkering die curl bij Vlakglas, TNO en MSD een 403 opleverde.

  python3 scripts/data_collection/haal_jaarverslagen.py --jaar 2025 --max 10
  python3 scripts/data_collection/haal_jaarverslagen.py --jaar 2025 --fondsen 51,72
  python3 scripts/data_collection/haal_jaarverslagen.py --jaar 2025 --via-site --max 8
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
import urllib.parse
import urllib.request
from collections import Counter

import fitz

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")
DOEL_MAP = os.path.join(BASE_DIR, "data", "annual_reports")
MIN_BYTES = 200_000
MIN_PAGINAS = 12
MAX_PAGINAS_JAARSCAN = 150   # ruim genoeg voor elk jaarverslag, en begrensd qua tijd
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

GENERIEK = {"pensioenfonds", "pensioen", "stichting", "bedrijfstakpensioenfonds", "fonds",
            "van", "de", "het", "en", "voor", "nederland", "nederlandse", "bpf", "spf",
            "apf", "ppi", "kring", "beroepspensioenfonds", "personeel", "medewerkers"}


def kenmerkend(naam: str) -> list[str]:
    zonder = re.sub(r"\([^)]*\)", " ", naam)
    return [w for w in re.findall(r"[A-Za-zÀ-ÿ]{3,}", zonder.lower()) if w not in GENERIEK]


def kies_url(con, fund_id: int, jaar: int) -> str | None:
    """Beste jaarverslag-URL voor dit fonds en boekjaar; verkorte versies laatst."""
    kandidaten = [r[0] for r in con.execute(
        "SELECT url FROM scraped_documents WHERE fund_id = ? AND lower(url) LIKE '%.pdf'",
        (fund_id,))]
    treffers = [u for u in kandidaten
                if str(jaar) in u
                and re.search(r"jaarverslag|jaarbericht|jaarrapport|jv_", u, re.I)]
    if not treffers:
        return None
    # Verkort, MVB- en infographic-versies zijn geen bruikbare bron.
    def straf(u: str) -> tuple:
        slecht = bool(re.search(r"verkort|mvb|verantwoord|populair|infograph|in.?beeld", u, re.I))
        return (slecht, -len(u))
    return sorted(treffers, key=straf)[0]


def zelfde_domein(url: str, website: str | None) -> bool:
    """Komt de PDF van het eigen domein van het fonds?

    Dat is een sterker bewijs van herkomst dan een naamtreffer in de tekst: het
    jaarverslag van SPMS heet 'Uw pensioen. Ons specialisme' en noemt de
    fondsnaam nergens op de eerste pagina's, maar staat wel op spms.nl.
    """
    if not website:
        return False
    def kern(u: str) -> str:
        host = urllib.parse.urlparse(u).netloc.lower()
        return host[4:] if host.startswith("www.") else host
    return bool(kern(url)) and kern(url) == kern(website)


def keur(pad: str, jaar: int, naam: str, van_eigen_site: bool = False) -> str | None:
    """None als het bestand deugt, anders de reden waarom niet."""
    if os.path.getsize(pad) < MIN_BYTES:
        return f"te klein ({os.path.getsize(pad):,} bytes)"
    with open(pad, "rb") as f:
        if f.read(4) != b"%PDF":
            return "geen PDF-header"
    try:
        doc = fitz.open(pad)
        n = len(doc)
        tekst = re.sub(r"\s+", " ", " ".join(doc[i].get_text() for i in range(min(6, n)))).lower()
        # Voor het boekjaar het hele document lezen, niet alleen de omslag. Metro's
        # verslag over 2024 kwam anders binnen als 2025: de eerste zes pagina's
        # bevatten geen enkel verslag-woord met jaartal, waardoor de controle
        # terugviel op "staat 2025 ergens" — en dat klopte, want het stuk is
        # ondertekend op 20 juni 2025. Over het geheel noemt het negen keer 2024.
        heel = re.sub(r"\s+", " ", " ".join(
            doc[i].get_text() for i in range(min(MAX_PAGINAS_JAARSCAN, n)))).lower()
        doc.close()
    except Exception as e:
        return f"onleesbaar ({type(e).__name__})"
    # Een jaarverslag van een pensioenfonds loopt in de tientallen pagina's. Mars
    # leverde een PDF van twee pagina's en 1.868 tekens — een verkorte
    # samenvatting, groot genoeg om door de bytegrens te komen omdat er een
    # paginavullende afbeelding in zat. Daar valt geen analyse uit te schrijven.
    if n < MIN_PAGINAS:
        return f"te kort voor een jaarverslag ({n} pagina's)"
    # Waar het misgaat is een verslag dat een ánder boekjaar draagt dan gevraagd:
    # TNO's verslag over 2024 kwam binnen als 2025 omdat de URL een uploaddatum
    # bevatte en '2025' verderop in het document stond. Alleen kijken of het
    # jaartal érgens voorkomt, is dus te zwak.
    #
    # Eisen dat het jaartal aan een verslag-woord op de omslag vastzit, is
    # daarentegen te streng: BPL zet '20 25' als typografisch element over twee
    # regels, Bakkersbedrijf opent met een stempel van de accountant en Achmea
    # met 'PENSIOENFONDS ACHMEA 2025'. Alle drie deugen.
    #
    # Dus: een tegenstrijdig boekjaar is een afkeuring, een ontbrekend niet.
    # Welk jaar het verslag draagt, blijkt uit hoe vaak een jaartal aan een
    # verslag-woord vastzit — niet uit het laagste (elk verslag over 2025 noemt
    # 2024 als vergelijkingsjaar) en niet uit het eerste (bij SBZ won een regel
    # uit de inhoudsopgave, "opvolging aanbevelingen boekjaar 2024 in 2025", het
    # van de omslag). SBZ noemt "jaarverslag 2025" vier keer en 2024 één keer.
    # Ook Engelse verslag-woorden: BP's fonds publiceert als Belgische OFP in het
    # Engels, en zonder die termen vond de controle geen enkel boekjaar. Dat
    # verslag ("annual report for the year ended 31 December 2024") kwam daardoor
    # binnen als 2025.
    verslag = (r"(?:jaarverslag|jaarbericht|jaarrapport|jaarrekening|verslagjaar|boekjaar"
               r"|annual report|annual accounts|financial year|year ended)")
    # Tussen het verslag-woord en het jaartal mogen cijfers staan: "annual report
    # for the year ended 31 December 2024" liep stuk op een \D-begrenzing, en
    # daardoor kwam BP's verslag over 2024 binnen als 2025. De meerderheidstelling
    # hieronder vangt de ruis op die dit erbij haalt.
    treffers = [int(m.group(1)) for m in
                re.finditer(rf"{verslag}.{{0,40}}?(20\d{{2}})", heel)]
    treffers += [int(m.group(1)) for m in
                 re.finditer(rf"(20\d{{2}}).{{0,25}}?{verslag}", heel)]
    if treffers:
        telling = Counter(treffers)
        draagt = max(telling, key=lambda j: (telling[j], j))
        if draagt != jaar:
            return f"draagt boekjaar {draagt}, niet {jaar} ({dict(telling)})"
    elif str(jaar) not in heel:
        return f"boekjaar {jaar} komt in het document niet voor"
    # Een kring binnen een algemeen pensioenfonds moet altijd op naam kloppen, ook
    # als de PDF van het eigen domein komt: stappensioen.nl host de deelverslagen
    # van tien kringen, en het domein zegt dus niets over wélke. Zo kwam onder
    # 'Pensioenkring Randstad' het deel-jaarverslag van Pensioenkring Ballast
    # Nedam binnen — een ander fonds in onze tabel, met eigen cijfers.
    if re.search(r"\b(pensioen)?kring\b", naam, re.I):
        van_eigen_site = False
    woorden = kenmerkend(naam)
    if not van_eigen_site and woorden and not any(
            re.search(rf"\b{re.escape(w)}\b", tekst) for w in woorden):
        return f"fondsnaam komt niet voor (gezocht op {', '.join(woorden[:3])})"
    return None


# Rangschikking van kandidaatpagina's: een documentenpagina eerst. Zonder deze
# volgorde bleef de crawl hangen op 'over-pensioen'-pagina's en bereikte hij
# /documenten nooit, terwijl het verslag daar gewoon stond.
PAGINA_SCORE = [
    (re.compile(r"/documenten|/publicaties|/downloads", re.I), 0),
    (re.compile(r"document|publicat|download", re.I), 1),
    (re.compile(r"jaarverslag|jaarbericht", re.I), 2),
    (re.compile(r"financ|over-ons|over_ons", re.I), 3),
]
# Paden die veel fondssites hebben maar niet altijd vanaf de homepage linken.
VASTE_PADEN = ["documenten", "over-ons/documenten", "publicaties", "downloads",
               "over-ons/publicaties", "over-ons/jaarverslagen", "jaarverslagen"]
NIET_HET_VERSLAG = re.compile(r"verkort|mvb|verantwoord|populair|infograph|in.?beeld|beleid", re.I)

FETCH_JS = """async (u) => {
  const r = await fetch(u, {credentials: 'include'});
  const b = new Uint8Array(await r.arrayBuffer());
  let s = '', CH = 8192;
  for (let i = 0; i < b.length; i += CH) s += String.fromCharCode.apply(null, b.subarray(i, i + CH));
  return [r.status, btoa(s)];
}"""


def zoek_en_haal_via_site(pg, home: str, jaar: int) -> tuple[str, bytes] | None:
    """Loop de documentenpagina van het fonds af en haal het jaarverslag op.

    De download gaat met fetch() binnen de pagina in plaats van via een losse
    request: die draagt de cookies en de fingerprint van een echte browser, en
    komt daarmee langs de WAF die curl afwijst.
    """
    try:
        pg.goto(home, wait_until="domcontentloaded", timeout=45000)
        pg.wait_for_timeout(1800)
        links = [h for h in dict.fromkeys(pg.eval_on_selector_all("a[href]", "e=>e.map(x=>x.href)")) if h]
    except Exception:
        return None

    def score(u: str) -> int:
        for patroon, punten in PAGINA_SCORE:
            if patroon.search(u):
                return punten
        return 9

    te_bezoeken = sorted([h for h in links if score(h) < 9], key=score)[:6]
    te_bezoeken += [home.rstrip("/") + "/" + pad for pad in VASTE_PADEN]

    kandidaten: set[str] = set()
    gezien: set[str] = set()
    for pagina in te_bezoeken:
        if pagina in gezien:
            continue
        gezien.add(pagina)
        try:
            r = pg.goto(pagina, wait_until="domcontentloaded", timeout=30000)
            if not r or r.status >= 400:
                continue
            pg.wait_for_timeout(1200)
            for h in pg.eval_on_selector_all("a[href]", "e=>e.map(x=>x.href)"):
                if (h and ".pdf" in h.lower()
                        and re.search(r"jaarverslag|jaarbericht|jaarrapport|jv_", h, re.I)
                        and not NIET_HET_VERSLAG.search(h)):
                    kandidaten.add(h)
            if kandidaten:
                break
        except Exception:
            continue
    if not kandidaten:
        return None

    def jaartal(u: str) -> int:
        gevonden = re.findall(r"(20[12]\d)", u.rsplit("/", 1)[-1])
        return max(int(x) for x in gevonden) if gevonden else 0

    # Voorkeur voor het gevraagde boekjaar; anders het nieuwste dat er is.
    volgorde = sorted(kandidaten, key=lambda u: (jaartal(u) != jaar, -jaartal(u)))
    for url in volgorde[:2]:
        try:
            status, b64 = pg.evaluate(FETCH_JS, url)
            if status == 200:
                import base64
                data = base64.b64decode(b64)
                if data[:4] == b"%PDF":
                    return url, data
        except Exception:
            continue
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jaar", type=int, default=2025)
    ap.add_argument("--max", type=int, default=10, help="hoeveel fondsen deze run")
    ap.add_argument("--fondsen", type=str, default="", help="komma-gescheiden fonds-ids")
    ap.add_argument("--via-site", action="store_true",
                    help="doorloop de eigen site als scraped_documents geen URL heeft")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    if args.fondsen:
        ids = [int(x) for x in args.fondsen.split(",") if x.strip()]
        doelen = [(fid,) + con.execute(
            "SELECT name, COALESCE(aum_euro_bn,0), website FROM funds WHERE id=?",
            (fid,)).fetchone() for fid in ids]
    else:
        doelen = con.execute("""
            SELECT a.fund_id, f.name, COALESCE(f.aum_euro_bn, 0), f.website FROM fund_analysis a
            JOIN funds f ON f.id = a.fund_id
            WHERE a.fiscal_year = (SELECT MAX(fiscal_year) FROM fund_analysis WHERE fund_id = a.fund_id)
              AND a.fiscal_year < ? AND COALESCE(f.is_pensioenfonds, 1) = 1
            ORDER BY COALESCE(f.aum_euro_bn, 0) DESC LIMIT ?""", (args.jaar, args.max)).fetchall()

    os.makedirs(DOEL_MAP, exist_ok=True)
    goed = afgekeurd = geen_url = 0
    browser = context = pg = None
    if args.via_site:
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=UA, locale="nl-NL")
        pg = context.new_page()

    for fid, naam, aum, website in doelen:
        kort = re.sub(r"[^A-Za-z0-9]+", "_", naam.split("(")[0].strip())[:24].strip("_")
        pad = os.path.join(DOEL_MAP, f"{fid}_{kort}_{args.jaar}.pdf")
        url = kies_url(con, fid, args.jaar)
        data = None

        if url:
            try:
                req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/pdf,*/*"})
                with urllib.request.urlopen(req, timeout=120) as r:
                    data = r.read()
            except Exception as e:
                print(f"  {fid:>4} {naam[:34]:<35} directe download faalde ({type(e).__name__})"
                      + (" — via de site proberen" if pg else ""))

        if data is None and pg and website:
            gevonden = zoek_en_haal_via_site(pg, website, args.jaar)
            if gevonden:
                url, data = gevonden

        if data is None:
            print(f"  {fid:>4} {naam[:34]:<35} geen {args.jaar}-verslag gevonden")
            geen_url += 1
            continue

        with open(pad, "wb") as f:
            f.write(data)
        reden = keur(pad, args.jaar, naam, zelfde_domein(url, website))
        if reden:
            os.remove(pad)
            print(f"  {fid:>4} {naam[:34]:<35} AFGEKEURD: {reden}")
            afgekeurd += 1
        else:
            print(f"  {fid:>4} {naam[:34]:<35} ok  {os.path.getsize(pad)//1024} kB  {os.path.basename(pad)}")
            goed += 1

    if browser:
        browser.close()
    print(f"\n{goed} opgehaald, {afgekeurd} afgekeurd, {geen_url} zonder bekende URL")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
