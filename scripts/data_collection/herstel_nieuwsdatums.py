"""Haal titel en publicatiedatum alsnog op voor berichten waar dat mislukte.

Van de 3.218 nieuwsberichten hadden er 777 geen datum en 420 als titel
"Challenge Validation". Dat laatste is geen titel maar de blokkadepagina van
een WAF: `parse_news_articles.py` haalt op met `requests`, wordt geweigerd, en
bewaart wat hij terugkrijgt. Bij 410 berichten vielen beide problemen samen.

De oplossing is dezelfde die vandaag ook bij de jaarverslagen werkte: een echte
browser. Playwright draagt de fingerprint en de cookies van Chrome en komt
daarmee langs de WAF die een kale request afwijst. In een steekproef van 25
berichten leverde dat er 23 alsnog een datum op.

Daarnaast worden vier datums leeggemaakt: 2025-01-01, 2025-12-31, 2026-01-01 en
2026-03-14. Op elk daarvan stonden tientallen tot honderden berichten, en dat
zijn geen publicatiedagen maar noodwaarden — 138 van de 148 berichten op
31 december horen bij één fonds. Ze zijn niet te onderscheiden van een echte
publicatie op die dag en vervuilen elke sortering; eerder kregen twee analyses
daardoor een publicatiedatum van 1 januari 2026 terwijl hun verslag in juni
verscheen. Leeg is eerlijker dan verzonnen.

Drie grenzen bewaken de nieuwe datums. Een datum in de toekomst is een leesfout,
een datum voor 2005 hoort niet bij een nieuwsbericht van een fonds dat wij
volgen, en bij een generieke titel ("Nieuws", "Lees meer") gaat het om een
overzichtspagina, waar de datum van het bovenste bericht op staat en niet van
dit bericht.

  python3 scripts/data_collection/herstel_nieuwsdatums.py --max 40
  python3 scripts/data_collection/herstel_nieuwsdatums.py --apply --max 400
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import sqlite3
import sys
from datetime import date, datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

_spec = importlib.util.spec_from_file_location(
    "parse_news_articles", os.path.join(BASE_DIR, "scripts", "data_collection",
                                        "parse_news_articles.py"))
pn = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pn)

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

# Datums waarop zoveel berichten samenvallen dat het geen publicatiedag kan zijn.
NOODWAARDEN = ("2025-01-01", "2025-12-31", "2026-01-01", "2026-03-14")
# Titels die de pagina van een WAF of foutmelding zijn, niet van een bericht.
GEBLOKKEERD = ("Challenge Validation", "Just a moment...", "Attention Required! | Cloudflare",
               "Access denied", "403 Forbidden")
VROEGST = "2005-01-01"


def kandidaten(con, maximum: int):
    vragen = ",".join("?" * len(NOODWAARDEN))
    return con.execute(f"""
        SELECT n.rowid, n.fund_id, f.name, n.url, n.title, n.published_date
        FROM news_articles n JOIN funds f ON f.id = n.fund_id
        WHERE n.url LIKE 'http%'
          AND (n.published_date IS NULL
               OR n.published_date IN ({vragen})
               OR n.title IN ({",".join("?" * len(GEBLOKKEERD))}))
        ORDER BY n.fund_id, n.rowid LIMIT ?""",
        NOODWAARDEN + GEBLOKKEERD + (maximum,)).fetchall()


def lees(pg, url: str) -> tuple[str | None, str | None]:
    """(titel, datum) van één bericht, of (None, None) als de pagina niet laadt."""
    r = pg.goto(url, wait_until="domcontentloaded", timeout=20000)
    if not r or r.status >= 400:
        return None, None
    pg.wait_for_timeout(900)

    titel = pn.clean_title(pg.title())
    if titel in GEBLOKKEERD:
        return None, None

    # Eerst de gestructureerde bronnen: daar staat de datum met opzet, en een
    # datum van vandaag is er te vertrouwen. In lopende tekst is "vandaag" veel
    # vaker een voettekst met de laatste wijziging dan een echte publicatiedag.
    for selector, attribuut in (("time[datetime]", "datetime"),
                                ("meta[property='article:published_time']", "content"),
                                ("meta[name='date']", "content"),
                                ("meta[itemprop='datePublished']", "content")):
        el = pg.locator(selector).first
        if el.count():
            d = pn.parse_published_date(el.get_attribute(attribuut) or "", trust_today=True)
            if d:
                return titel, d
    return titel, pn.parse_published_date(pg.inner_text("body"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="schrijf de gevonden waarden weg")
    ap.add_argument("--max", type=int, default=40, help="hoeveel berichten deze run")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH, timeout=60)
    rijen = kandidaten(con, args.max)
    vandaag = date.today().isoformat()
    print(f"{len(rijen)} berichten opnieuw ophalen\n")

    from playwright.sync_api import sync_playwright
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    pg = browser.new_context(user_agent=UA, locale="nl-NL").new_page()

    nieuw_datum = nieuw_titel = mislukt = geweigerd = 0
    voorstel = []
    try:
        for rid, fid, fondsnaam, url, oude_titel, oude_datum in rijen:
            try:
                titel, datum = lees(pg, url)
            except Exception as e:
                print(f"  {fid:>4} {type(e).__name__:<18} {url[:60]}")
                mislukt += 1
                continue
            if titel is None and datum is None:
                mislukt += 1
                continue
            # Een generieke titel hoort bij een overzichtspagina; de datum die
            # daar staat is van het bovenste bericht, niet van dit bericht.
            if datum and pn.looks_generic(titel):
                datum, geweigerd = None, geweigerd + 1
            if datum and not (VROEGST <= datum <= vandaag):
                datum, geweigerd = None, geweigerd + 1
            zet_titel = titel if (titel and (oude_titel in GEBLOKKEERD
                                             or pn.looks_generic(oude_titel))) else None
            if not datum and not zet_titel:
                continue
            voorstel.append((rid, datum, zet_titel))
            if datum:
                nieuw_datum += 1
            if zet_titel:
                nieuw_titel += 1
            print(f"  {fid:>4} {fondsnaam[:20]:<22} {str(oude_datum or '-'):<11} -> "
                  f"{datum or '-':<11} {(zet_titel or oude_titel or '')[:38]}")
    finally:
        browser.close()
        pw.stop()

    print(f"\n{nieuw_datum} datums en {nieuw_titel} titels gevonden; "
          f"{geweigerd} datums afgewezen door de grenzen, {mislukt} pagina's onbereikbaar")

    if not args.apply:
        print("\nDroogloop. Draai met --apply om dit weg te schrijven.")
        con.close()
        return 0

    kopie = DB_PATH.replace(".db", f".backup-{datetime.now():%Y%m%d_%H%M%S}.db")
    shutil.copy2(DB_PATH, kopie)
    print(f"Back-up: {os.path.basename(kopie)}")

    leeg = con.execute(
        f"UPDATE news_articles SET published_date = NULL WHERE published_date IN "
        f"({','.join('?' * len(NOODWAARDEN))})", NOODWAARDEN).rowcount
    for rid, datum, titel in voorstel:
        if datum:
            con.execute("UPDATE news_articles SET published_date=? WHERE rowid=?", (datum, rid))
        if titel:
            con.execute("UPDATE news_articles SET title=? WHERE rowid=?", (titel, rid))
    con.commit()
    print(f"{leeg} noodwaarden leeggemaakt, {nieuw_datum} datums en {nieuw_titel} titels gezet.")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
