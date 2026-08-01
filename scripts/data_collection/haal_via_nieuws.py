"""Haal een jaarverslag op via het nieuwsbericht waarin het wordt aangekondigd.

De bestaande ophaler zoekt op documentenpagina's. Dat werkt bij fondsen die een
overzichtspagina hebben, maar niet bij fondsen die hun verslag alleen aankondigen
in een nieuwsbericht en daar naar de PDF linken. Tien fondsen met een gepubliceerd
verslag over 2025 bleven daardoor buiten beeld, terwijl `news_articles` de URL van
dat bericht gewoon bevat — inclusief de publicatiedatum.

Die datum is de tweede opbrengst. `fund_analysis.source_published_date` was bij
189 van de 206 analyses leeg, en juist daarop sorteert de analysepagina. Het
nieuwsbericht weet wanneer het verslag verscheen; het verslag zelf zegt dat
nergens eenduidig.

  python3 scripts/data_collection/haal_via_nieuws.py --jaar 2025
  python3 scripts/data_collection/haal_via_nieuws.py --jaar 2025 --apply
"""

from __future__ import annotations

import argparse
import base64
import importlib.util
import os
import re
import sqlite3
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

_spec = importlib.util.spec_from_file_location(
    "haal_jaarverslagen", os.path.join(BASE_DIR, "scripts", "data_collection",
                                       "haal_jaarverslagen.py"))
hj = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hj)

# Een verkorte versie of een MVB-verslag is niet het jaarverslag.
NIET_HET_VERSLAG = re.compile(r"verkort|mvb|maatschappelijk|populair|infograph|in.?beeld", re.I)
FETCH_JS = """async (u) => {
  const r = await fetch(u, {credentials: 'include'});
  const b = new Uint8Array(await r.arrayBuffer());
  let s = '', CH = 8192;
  for (let i = 0; i < b.length; i += CH) s += String.fromCharCode.apply(null, b.subarray(i, i + CH));
  return [r.status, btoa(s)];
}"""


def kandidaten(con, jaar: int):
    """Fondsen met een nieuwsbericht over het jaarverslag maar zonder analyse."""
    return con.execute("""
        SELECT n.fund_id, f.name, MAX(n.published_date) datum, MIN(n.url) url
        FROM news_articles n JOIN funds f ON f.id = n.fund_id
        WHERE n.title LIKE '%jaarverslag%' AND n.published_date >= ?
          AND COALESCE(f.is_pensioenfonds, 1) = 1
          AND NOT EXISTS (SELECT 1 FROM fund_analysis a
                          WHERE a.fund_id = n.fund_id AND a.fiscal_year = ?)
        GROUP BY n.fund_id ORDER BY datum DESC""",
        (f"{jaar}-01-01", jaar)).fetchall()


# Steeds meer fondsen publiceren hun jaarverslag als website in plaats van als
# document: verslagen.uwvpensioen.nl/jaarverslag-2025 bijvoorbeeld. Zo'n site
# heeft doorgaans een aparte pagina die het geheel als PDF aanbiedt. Zonder die
# tweede stap blijft het bericht "linkt geen PDF", terwijl het verslag er wel is.
NAAR_VERSLAGSITE = re.compile(r"verslag|jaarverslag-20\d\d|annualreport", re.I)
NAAR_DOWNLOAD = re.compile(r"downloaden-als-pdf|download.{0,12}pdf|pdf.{0,12}download|/print", re.I)


def _links(pg, url: str, wacht: int = 1500):
    r = pg.goto(url, wait_until="domcontentloaded", timeout=40000)
    if not r or r.status >= 400:
        return None, r.status if r else None
    pg.wait_for_timeout(wacht)
    return [h for h in dict.fromkeys(pg.eval_on_selector_all("a[href]", "e=>e.map(x=>x.href)")) if h], r.status


def haal(pg, nieuws_url: str, jaar: int):
    """Open het nieuwsbericht en haal de PDF waar het naar linkt.

    Loopt zo nodig twee stappen door: bericht -> online jaarverslag -> pagina die
    het als PDF aanbiedt.
    """
    try:
        links, status = _links(pg, nieuws_url)
        if links is None:
            return None, f"nieuwspagina gaf {status}"
    except Exception as e:
        return None, f"nieuwspagina onbereikbaar ({type(e).__name__})"

    pdfs = [h for h in links if ".pdf" in h.lower()]
    if not pdfs:
        # Stap 2: een online jaarverslag op een eigen (sub)domein.
        eigen = nieuws_url.split("/")[2]
        kandidaten = [h for h in links if NAAR_VERSLAGSITE.search(h) and h.split("/")[2] != eigen]
        kandidaten += [h for h in links if NAAR_VERSLAGSITE.search(h) and str(jaar) in h]
        for site in list(dict.fromkeys(kandidaten))[:3]:
            try:
                sublinks, _ = _links(pg, site, 2000)
                if not sublinks:
                    continue
                pdfs = [h for h in sublinks if ".pdf" in h.lower()]
                if pdfs:
                    break
                # Stap 3: de 'downloaden als pdf'-pagina van die verslagsite.
                for dl in [h for h in sublinks if NAAR_DOWNLOAD.search(h)][:2]:
                    diep, _ = _links(pg, dl, 2500)
                    pdfs = [h for h in (diep or []) if ".pdf" in h.lower()]
                    if pdfs:
                        break
                if pdfs:
                    break
            except Exception:
                continue
    if not pdfs:
        return None, "nieuwsbericht linkt geen PDF, ook niet via een verslagsite"
    # Het volledige verslag heeft voorrang op een verkorte of MVB-versie.
    volgorde = sorted(pdfs, key=lambda u: (bool(NIET_HET_VERSLAG.search(u)),
                                           str(jaar) not in u, -len(u)))
    for url in volgorde[:3]:
        try:
            status, b64 = pg.evaluate(FETCH_JS, url)
            if status == 200:
                data = base64.b64decode(b64)
                if data[:4] == b"%PDF":
                    return (url, data), None
        except Exception:
            continue
    return None, f"{len(pdfs)} PDF-links, geen bruikbare"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jaar", type=int, default=2025)
    ap.add_argument("--apply", action="store_true", help="sla op wat er binnenkomt")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    rijen = kandidaten(con, args.jaar)
    print(f"{len(rijen)} fondsen kondigden een jaarverslag {args.jaar} aan zonder dat wij "
          f"een analyse hebben\n")
    if not args.apply:
        for fid, naam, datum, url in rijen:
            print(f"  {fid:>4} {naam[:32]:<34} {datum[:10]}  {url[:64]}")
        print("\nDroogloop. Draai met --apply om de verslagen op te halen.")
        con.close()
        return 0

    from playwright.sync_api import sync_playwright
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    pg = browser.new_context(user_agent=hj.UA, locale="nl-NL").new_page()

    goed = mis = 0
    try:
        for fid, naam, datum, url in rijen:
            gevonden, reden = haal(pg, url, args.jaar)
            if not gevonden:
                print(f"  {fid:>4} {naam[:30]:<32} {reden}")
                mis += 1
                continue
            pdf_url, data = gevonden
            kort = re.sub(r"[^A-Za-z0-9]+", "_", naam.split("(")[0].strip())[:24].strip("_")
            pad = os.path.join(hj.DOEL_MAP, f"{fid}_{kort}_{args.jaar}.pdf")
            with open(pad, "wb") as f:
                f.write(data)
            afkeur = hj.keur(pad, args.jaar, naam, hj.zelfde_domein(pdf_url, url))
            if afkeur:
                os.remove(pad)
                print(f"  {fid:>4} {naam[:30]:<32} afgekeurd: {afkeur[:46]}")
                mis += 1
                continue
            con.execute("""INSERT OR REPLACE INTO ophaal_wachtrij
                (fund_id, jaar, status, pogingen, reden, url, pad, bijgewerkt)
                VALUES (?,?,'binnen',1,NULL,?,?,datetime('now'))""",
                        (fid, args.jaar, pdf_url, os.path.relpath(pad, BASE_DIR)))
            con.commit()
            print(f"  {fid:>4} {naam[:30]:<32} ok  {os.path.getsize(pad)//1024} kB  "
                  f"gepubliceerd {datum[:10]}")
            goed += 1
    finally:
        browser.close()
        pw.stop()
    print(f"\n{goed} opgehaald, {mis} niet gelukt")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
