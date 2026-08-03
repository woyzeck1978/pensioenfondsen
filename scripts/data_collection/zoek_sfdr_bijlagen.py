"""Zoek de SFDR-duurzaamheidsinformatie op de site van elk fonds.

Onder de SFDR moet elke financiëlemarktdeelnemer — en een pensioenfonds is dat —
informatie over duurzaamheid op zijn website publiceren. Voor artikel 8- en
9-producten hoort daar de precontractuele bijlage bij, en die draagt een vast
sjabloon waaruit de classificatie zonder interpretatie volgt. Elk fonds heeft
zo'n pagina dus; de vraag is alleen waar.

Niet op de homepage. Van de acht grootste fondsen zonder `sfdr_article` linkten
er vijf vanaf hun voorpagina helemaal niet naar duurzaamheid. Bij SPMS staat de
bijlage twee klikken diep, op /over-spms/beleggen/verantwoord-beleggen/
rapportages, en daar heet hij "SFDR Duurzaamheidsinformatie" naast
"Precontractuele informatie SPMS". De monitor bezoekt een vast lijstje paden en
komt daar nooit.

Dit script loopt daarom drie sporen af, in deze volgorde omdat ze steeds meer
werk kosten:

  1. de vaste paden die fondsen voor dit onderwerp gebruiken;
  2. links op de homepage die over duurzaamheid gaan, één hop diep gevolgd;
  3. de sitemap, die vaak pagina's bevat waar niets naar linkt.

Wat het vindt gaat als document de tabel in, zodat het niet elke keer opnieuw
gezocht hoeft te worden. De classificatie zelf leidt
`bepaal_sfdr_uit_bijlage.py` eruit af.

  python3 scripts/data_collection/zoek_sfdr_bijlagen.py --max 10
  python3 scripts/data_collection/zoek_sfdr_bijlagen.py --apply --max 100
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
from urllib.parse import urlparse

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

VASTE_PADEN = [
    # Met en zonder .html: PFZW en andere Sitecore-sites hangen er een extensie aan.
    "/duurzaamheid", "/verantwoord-beleggen", "/beleggingen/verantwoord-beleggen",
    "/over-ons/verantwoord-beleggen", "/sfdr", "/duurzaamheidsinformatie",
    "/over-ons/duurzaamheid", "/beleggen/verantwoord-beleggen",
    "/maatschappelijk-verantwoord-beleggen", "/over-ons/beleggingen",
    "/over-ons/duurzaam-beleggen.html", "/duurzaam-beleggen.html",
    "/verantwoord-beleggen.html", "/duurzaamheid.html",
]
# Links waarvan het de moeite is ze één hop te volgen.
NAAR_DUURZAAM = re.compile(r"duurzaam|verantwoord.?belegg|sustainab|sfdr|\besg\b|\bmvb\b|rapportage",
                           re.I)
# Secties waaronder de duurzaamheidspagina vaak hangt zonder dat de homepage er
# rechtstreeks naar linkt.
NAAR_SECTIE = re.compile(r"/over[-_]|beleggen|beleggingen|financieel|publicat|document", re.I)
# Documenten die de bijlage of de website-disclosure kunnen zijn.
IS_BIJLAGE = re.compile(r"sfdr|duurzaamheidsinformatie|precontractue|periodieke informatie|"
                        r"annex[ _-]*[iv1-5]|sustainability.related", re.I)
COOKIE = ("#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll", "#onetrust-accept-btn-handler",
          "button:has-text('Alles toestaan')", "button:has-text('Accepteer')",
          "button:has-text('Akkoord')", "button:has-text('Toestaan')")


def _links(pg, url: str, wacht: int = 1400):
    r = pg.goto(url, wait_until="domcontentloaded", timeout=22000)
    if not r or r.status >= 400:
        return None
    pg.wait_for_timeout(wacht)
    for sel in COOKIE:
        try:
            if pg.locator(sel).count():
                pg.locator(sel).first.click(timeout=2500)
                pg.wait_for_timeout(1000)
                break
        except Exception:
            pass
    return pg.eval_on_selector_all(
        "a[href]", "e=>e.map(x=>x.href+'|'+(x.innerText||'').trim().replace(/\\s+/g,' '))")


def _bijlagen(links) -> list[tuple[str, str]]:
    uit = []
    for l in links or []:
        u, _, t = l.partition("|")
        if u.endswith("#") or not u.startswith("http"):
            continue
        if IS_BIJLAGE.search(f"{t} {u}"):
            uit.append((u, t or ""))
    return uit


def zoek(pg, home: str) -> list[tuple[str, str]]:
    p = urlparse(home)
    basis = f"{p.scheme}://{p.netloc}"
    gevonden: dict[str, str] = {}

    # 1. vaste paden
    for pad in VASTE_PADEN:
        try:
            for u, t in _bijlagen(_links(pg, basis + pad)):
                gevonden.setdefault(u, t)
        except Exception:
            continue
        if gevonden:
            return list(gevonden.items())

    # 2. homepage, en één hop door de duurzaamheidslinks
    try:
        links = _links(pg, home, 1800)
        for u, t in _bijlagen(links):
            gevonden.setdefault(u, t)
        # Ook de 'over ons'- en beleggingspagina's volgen, niet alleen links die
        # zelf over duurzaamheid gaan. PFZW linkt vanaf de homepage uitsluitend
        # naar "Over PFZW"; het duurzaamheidsdeel hangt daaronder en zou anders
        # onbereikbaar blijven.
        diep = [l.split("|")[0] for l in (links or [])
                if NAAR_DUURZAAM.search(l) and p.netloc in l and not l.split("|")[0].endswith("#")]
        diep += [l.split("|")[0] for l in (links or [])
                 if NAAR_SECTIE.search(l) and p.netloc in l and not l.split("|")[0].endswith("#")]
        for u in list(dict.fromkeys(diep))[:10]:
            try:
                sub = _links(pg, u)
                for du, dt in _bijlagen(sub):
                    gevonden.setdefault(du, dt)
                # nog één hop: 'rapportages' hangt vaak ónder verantwoord-beleggen
                for vu in [l.split("|")[0] for l in (sub or [])
                           if NAAR_DUURZAAM.search(l) and p.netloc in l][:4]:
                    if vu in gevonden or vu == u:
                        continue
                    for eu, et in _bijlagen(_links(pg, vu)):
                        gevonden.setdefault(eu, et)
            except Exception:
                continue
    except Exception:
        pass
    if gevonden:
        return list(gevonden.items())

    # 3. sitemap: pagina's waar niets naar linkt
    for sm in ("/sitemap.xml", "/sitemap_index.xml"):
        try:
            r = pg.goto(basis + sm, wait_until="domcontentloaded", timeout=20000)
            if not r or r.status >= 400:
                continue
            urls = re.findall(r"https?://[^\s<\"]+", pg.inner_text("body"))
            for u in [x for x in urls if NAAR_DUURZAAM.search(x)][:5]:
                for du, dt in _bijlagen(_links(pg, u)):
                    gevonden.setdefault(du, dt)
            break
        except Exception:
            continue
    return list(gevonden.items())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="bewaar de gevonden documenten")
    ap.add_argument("--max", type=int, default=10, help="hoeveel fondsen deze run")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH, timeout=60)
    rijen = con.execute("""
        SELECT id, name, website, COALESCE(aum_euro_bn, 0) FROM funds
        WHERE COALESCE(is_pensioenfonds, 1) = 1
          AND COALESCE(status,'') NOT LIKE 'Duplicaat%'
          AND sfdr_article IS NULL AND website IS NOT NULL
        ORDER BY COALESCE(aum_euro_bn, 0) DESC LIMIT ?""", (args.max,)).fetchall()
    print(f"{len(rijen)} fondsen zonder SFDR-artikel\n")

    from playwright.sync_api import sync_playwright
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=False)
    pg = browser.new_context(user_agent=UA, locale="nl-NL",
                             viewport={"width": 1400, "height": 900}).new_page()

    nieuw = met = zonder = 0
    try:
        for fid, naam, site, aum in rijen:
            try:
                treffers = zoek(pg, site)
            except Exception as e:
                print(f"  {fid:>4} {naam[:30]:<32} {type(e).__name__}")
                zonder += 1
                continue
            if not treffers:
                print(f"  {fid:>4} {naam[:30]:<32} niets gevonden")
                zonder += 1
                continue
            met += 1
            print(f"  {fid:>4} {naam[:30]:<32} {len(treffers)} document(en)")
            for u, t in treffers[:3]:
                print(f"        {t[:36]:<38} {u[:70]}")
            if args.apply:
                for u, t in treffers:
                    cur = con.execute(
                        "INSERT OR IGNORE INTO scraped_documents (fund_id, url, title, doc_type) "
                        "VALUES (?,?,?,'document')", (fid, u, t or "SFDR-document"))
                    nieuw += cur.rowcount
                con.commit()
    finally:
        browser.close()
        pw.stop()

    print(f"\n{met} fondsen met een vindbare bijlage, {zonder} zonder")
    if args.apply:
        print(f"{nieuw} documenten toegevoegd. Draai daarna "
              f"scripts/db_management/bepaal_sfdr_uit_bijlage.py om het artikel af te leiden.")
    else:
        print("\nDroogloop. Draai met --apply om de documenten te bewaren.")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
