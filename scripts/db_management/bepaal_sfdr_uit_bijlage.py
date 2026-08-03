"""Leid het SFDR-artikel af uit de precontractuele of periodieke bijlage.

ROADMAP-punt 6 sloot in juli 2026 met de conclusie dat `sfdr_article` niet uit
jaarverslagen te halen is. Dat klopt nog steeds: PFZW schrijft over
'beleggingen die niet geclassificeerd zijn als artikel 8 SFDR product' en SPMS
wil 'een groter deel van de portefeuille classificeren als artikel 8'. Beide
gaan over de beleggingen ín de portefeuille, niet over het fonds zelf, en wie
op 'artikel 8' zoekt schrijft ze allebei fout weg.

De bijlagen zijn wél eenduidig, en die staan inmiddels in `scraped_documents`.
De SFDR-verordening schrijft een vast sjabloon voor, en de kop van dat sjabloon
verraadt de classificatie zonder dat er iets te interpreteren valt:

  "Ecologische en/of sociale kenmerken (E/S-kenmerken)"  -> bijlage II of IV
                                                         -> artikel 8
  "Duurzame beleggingsdoelstelling"                      -> bijlage III of V
                                                         -> artikel 9

Alleen artikel 8- en 9-producten publiceren zo'n bijlage; een artikel 6-fonds
heeft er geen. Dat maakt de toets eenzijdig: een bijlage bewijst 8 of 9, maar
het ontbreken ervan bewijst niets, want veel fondsen publiceren de bijlage op
een plek die de monitor niet bezoekt.

Tellen hoe vaak elke term voorkomt werkt niet. Het sjabloon vraagt zelf "Heeft
dit financiële product een duurzame beleggingsdoelstelling? Ja / Nee" en noemt
dus altijd beide mogelijkheden. Daarom kijkt dit script alleen naar de kop van
de eerste pagina, waar één van de twee als titel staat.

  python3 scripts/db_management/bepaal_sfdr_uit_bijlage.py
  python3 scripts/db_management/bepaal_sfdr_uit_bijlage.py --apply
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sqlite3
import sys
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

# Documenten die een SFDR-bijlage kunnen zijn.
BIJLAGE = re.compile(r"sfdr|precontractue|periodieke informatie|annex[ _-]*[iv1-5]", re.I)
# De koppen uit het verplichte sjabloon. Alleen in de eerste pagina zoeken.
KOP_ART8 = re.compile(r"ecologische\s+(?:en/of|of|en)\s+sociale\s+kenmerken", re.I)
# De ontkenning hoort bij de term. Centraal Beheer opent met "Geen duurzame
# beleggingsdoelstelling — het Pensioenfonds promoot ecologische of sociale
# kenmerken, maar heeft geen duurzame beleggingsdoelstelling". Dat is de
# tekstuele variant van het aangekruiste 'Nee'-vakje en dus artikel 8; wie de
# 'geen' wegleest maakt er artikel 9 van.
KOP_ART9 = re.compile(r"(?<!geen\s)(?<!niet\s)duurzame\s+beleggings(?:doelstelling|doel)", re.I)
ONTKEND = re.compile(r"(?:geen|niet)\s+(?:een\s+)?duurzame\s+beleggings(?:doelstelling|doel)", re.I)
KOPZONE = 1200


def kandidaten(con):
    return con.execute("""
        SELECT f.id, f.name, f.sfdr_article, s.title, s.url
        FROM funds f JOIN scraped_documents s ON s.fund_id = f.id
        WHERE COALESCE(f.is_pensioenfonds, 1) = 1
          AND COALESCE(f.status,'') NOT LIKE 'Duplicaat%'
        ORDER BY f.id""").fetchall()


def artikel_uit_kop(data: bytes) -> int | None:
    import fitz

    try:
        doc = fitz.open(stream=data, filetype="pdf")
        kop = re.sub(r"\s+", " ", doc[0].get_text())[:KOPZONE]
        doc.close()
    except Exception:
        return None
    # Welke van de twee als eerste staat, is de titel van het sjabloon. Niet
    # welke aanwezig is: het sjabloon vraagt even verderop zelf "Heeft dit
    # financiële product een duurzame beleggingsdoelstelling? Ja / Nee", dus in
    # elke artikel 8-bijlage staan beide termen. Alleen op aanwezigheid toetsen
    # gaf elf artikel 8-fondsen ten onrechte een 9, waaronder Pon, waarvan de
    # kop letterlijk "Ecologische en/of sociale kenmerken (E/S-kenmerken)" is.
    # Staat de term ontkend in de kop, dan is dat het 'Nee'-antwoord op de vraag
    # naar een duurzame beleggingsdoelstelling: artikel 8, ongeacht de volgorde.
    if ONTKEND.search(kop):
        return 8 if KOP_ART8.search(kop) else None
    m8, m9 = KOP_ART8.search(kop), KOP_ART9.search(kop)
    if m8 and m9:
        return 8 if m8.start() < m9.start() else 9
    if m9:
        return 9
    if m8:
        return 8
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="schrijf de gevonden artikelen weg")
    ap.add_argument("--max", type=int, default=60, help="hoeveel documenten deze run")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    rijen = [r for r in kandidaten(con) if BIJLAGE.search(f"{r[3] or ''} {r[4] or ''}")]
    # Per fonds hooguit een paar documenten proberen; de eerste die een kop
    # oplevert is genoeg.
    per_fonds: dict[int, list] = {}
    for fid, naam, art, titel, url in rijen:
        per_fonds.setdefault(fid, []).append((naam, art, titel, url))

    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "hj", os.path.join(BASE_DIR, "scripts", "data_collection", "haal_jaarverslagen.py"))
    hj = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(hj)
    from playwright.sync_api import sync_playwright

    voorstel, bevestigd, tegenspraak, niets = [], [], [], 0
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=False)
    pg = browser.new_context(user_agent=hj.UA, locale="nl-NL",
                             viewport={"width": 1400, "height": 900}).new_page()
    try:
        for fid, docs in list(per_fonds.items())[:args.max]:
            naam, art = docs[0][0], docs[0][1]
            gevonden = None
            for _naam, _art, titel, url in docs[:3]:
                data = hj.download(pg, url)
                if not data:
                    continue
                gevonden = artikel_uit_kop(data)
                if gevonden:
                    break
            if not gevonden:
                niets += 1
                continue
            if art is None:
                voorstel.append((fid, naam, gevonden))
                print(f"  {fid:>4} {naam[:34]:<36} leeg -> artikel {gevonden}")
            elif str(art).strip() == str(gevonden):
                bevestigd.append((fid, naam, gevonden))
            else:
                tegenspraak.append((fid, naam, art, gevonden))
                print(f"  {fid:>4} {naam[:34]:<36} tabel {art}, bijlage {gevonden}  TEGENSPRAAK")
    finally:
        browser.close()
        pw.stop()

    print(f"\n{len(voorstel)} in te vullen, {len(bevestigd)} bevestigd wat er al stond, "
          f"{len(tegenspraak)} tegenspraak, {niets} zonder leesbare kop")

    if not args.apply:
        print("\nDroogloop. Draai met --apply om dit weg te schrijven.")
        con.close()
        return 0

    kopie = DB_PATH.replace(".db", f".backup-{datetime.now():%Y%m%d_%H%M%S}.db")
    shutil.copy2(DB_PATH, kopie)
    print(f"Back-up: {os.path.basename(kopie)}")
    for fid, _naam, artikel in voorstel:
        con.execute("""UPDATE funds SET sfdr_article = ?,
            description = COALESCE(description,'') || ' SFDR-artikel afgeleid uit de eigen '
            || 'precontractuele of periodieke bijlage, niet uit het jaarverslag.'
            WHERE id = ? AND sfdr_article IS NULL""", (artikel, fid))
    con.commit()
    print(f"{len(voorstel)} fondsen bijgewerkt. Tegenspraken zijn niet aangepast; "
          f"die vragen om de hand.")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
