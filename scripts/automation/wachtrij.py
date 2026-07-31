"""Wachtrij voor het ophalen en voorbereiden van jaarverslagen.

Het werk valt uiteen in twee soorten. Het ophalen — documentenpagina's aflopen,
PDF's binnenhalen, keuren, de relevante passages uitsnijden — is mechanisch en
kost per fonds minuten aan browserwerk. Het schrijven van de analyse is dat niet:
daar is deze zomer gebleken dat automatiseren juist misgaat (boekjaar 0, verslagen
over het verkeerde bedrijf, een samenvatting met "115,x%" erin).

Dit script doet het eerste deel, onbeheerd, en laat het tweede over. Wat het
oplevert is per fonds een tekstbestand in data/interim/kern/ met de passages
waarin dekkingsgraad, rendement, toeslag, deelnemers en de Wtp-overgang staan.
Dat leest als een briefing van twee schermen in plaats van een verslag van
tweehonderd pagina's.

De stand staat in de database, niet in het geheugen van een draaiend proces:
een run die halverwege afbreekt kost hooguit het fonds dat op dat moment onder
handen was.

  python3 scripts/automation/wachtrij.py vul --jaar 2025
  python3 scripts/automation/wachtrij.py verwerk --jaar 2025 --minuten 45
  python3 scripts/automation/wachtrij.py stand
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import re
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")
KERN_MAP = os.path.join(BASE_DIR, "data", "interim", "kern")

# De ophaler hergebruiken in plaats van naschrijven: keur() bevat inmiddels het
# nodige dat je niet twee keer wilt onderhouden, zoals de boekjaarcontrole die
# TNO's verslag over 2024 onderschepte toen het als 2025 binnenkwam.
_spec = importlib.util.spec_from_file_location(
    "haal_jaarverslagen",
    os.path.join(BASE_DIR, "scripts", "data_collection", "haal_jaarverslagen.py"))
hj = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hj)

SCHEMA = """
CREATE TABLE IF NOT EXISTS ophaal_wachtrij (
    fund_id     INTEGER NOT NULL,
    jaar        INTEGER NOT NULL,
    status      TEXT    NOT NULL DEFAULT 'open',
    pogingen    INTEGER NOT NULL DEFAULT 0,
    reden       TEXT,
    url         TEXT,
    pad         TEXT,
    bijgewerkt  TEXT,
    PRIMARY KEY (fund_id, jaar)
)
"""

# open          nog niet geprobeerd
# binnen        PDF opgehaald en goedgekeurd, passages klaar — wacht op de analyse
# geanalyseerd  er staat een analyse in fund_analysis voor dit boekjaar
# niet_gevonden geen verslag over dit jaar te vinden (publiceert waarschijnlijk later)
# afgekeurd     wél iets gevonden, maar het deugde niet (verkeerd boekjaar, geen PDF)

# Waar de analyse over gaat. Per onderwerp een patroon; zinnen eromheen worden
# meegenomen zodat de context leesbaar blijft.
ONDERWERPEN = [
    ("dekkingsgraad", r"beleidsdekkingsgraad|actuele dekkingsgraad|vereiste dekkingsgraad|reële dekkingsgraad"),
    ("rendement", r"\brendement\b|beleggingsresultaat|performance"),
    ("toeslag", r"toeslag|indexatie|verhoging van de pensioenen|verlaging|korting"),
    ("vermogen", r"belegd vermogen|pensioenvermogen|balanstotaal|voorziening pensioenverplichtingen"),
    ("deelnemers", r"deelnemers|gepensioneerden|slapers|aangesloten werkgevers"),
    ("transitie", r"\bWtp\b|invaren|transitieplan|solidariteitsreserve|flexibele premieregeling|nieuwe regeling"),
    ("kosten", r"uitvoeringskosten|vermogensbeheerkosten|kosten per deelnemer|transactiekosten"),
    ("beleggingen", r"aandelen|vastrentend|vastgoed|matchingportefeuille|returnportefeuille|alternatives"),
]
MAX_PER_ONDERWERP = 12


def _nu() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _zet(con, fid: int, jaar: int, status: str, reden: str | None = None,
         url: str | None = None, pad: str | None = None) -> None:
    con.execute("""UPDATE ophaal_wachtrij
                   SET status=?, reden=?, url=COALESCE(?,url), pad=COALESCE(?,pad),
                       pogingen=pogingen+1, bijgewerkt=?
                   WHERE fund_id=? AND jaar=?""",
                (status, reden, url, pad, _nu(), fid, jaar))
    con.commit()


def snij_passages(pdf_pad: str, naam: str, jaar: int) -> str:
    """Snijd uit het verslag de zinnen waar de analyse over gaat."""
    import fitz

    doc = fitz.open(pdf_pad)
    tekst = re.sub(r"\s+", " ", " ".join(p.get_text() for p in doc))
    doc.close()

    # Sommige verslagen hebben een lettertype waarin de ligaturen verkeerd op
    # unicode zijn afgebeeld: PostNL levert "}nanciële" en "ezect". Een accolade
    # midden in een woord is altijd zo'n geval en kan blind terug; de rest niet,
    # want die tekens komen ook echt voor. Daarvoor een waarschuwing bovenaan de
    # briefing, zodat zo'n woord niet uit een citaat de samenvatting in glipt.
    tekst = re.sub(r"(?<=[a-zA-Z])\}(?=[a-zA-Z])", "fi", tekst)
    tekst = re.sub(r"\}(?=[a-z]{3,})", "fi", tekst)
    verdacht = len(re.findall(r"\b\w*[}{|]\w*\b|\b[a-z]*z[a-z]*ect\w*", tekst))

    # Zinnen met een getal erin: een bewering zonder cijfer voegt zelden iets toe
    # aan een analyse, en scheelt veel ruis.
    zinnen = re.split(r"(?<=[.!?])\s+", tekst)
    regels = [f"# {naam} — boekjaar {jaar}",
              f"# bron: {os.path.basename(pdf_pad)} ({len(tekst):,} tekens)".replace(",", "."),
              ""]
    if verdakt := verdacht:
        regels.insert(2, f"# LET OP: {verdakt} woorden met kapotte ligaturen — dit verslag heeft "
                         "een lettertype dat 'ff' en 'fi' verkeerd afbeeldt. Niet letterlijk overnemen.")
    for kop, patroon in ONDERWERPEN:
        pat = re.compile(patroon, re.I)
        heeft_getal = re.compile(r"\d")
        gekozen, gezien = [], set()
        for z in zinnen:
            z = z.strip()
            if not (30 < len(z) < 400) or not pat.search(z) or not heeft_getal.search(z):
                continue
            sleutel = z[:45].lower()
            if sleutel in gezien:
                continue
            gezien.add(sleutel)
            gekozen.append(z)
            if len(gekozen) >= MAX_PER_ONDERWERP:
                break
        regels.append(f"## {kop} ({len(gekozen)})")
        regels.extend(f"- {z}" for z in gekozen)
        regels.append("")
    return "\n".join(regels)


def _bestaand_verslag(fid: int, jaar: int, naam: str) -> str | None:
    """Staat het verslag al op schijf, en deugt het? Dan het pad, anders None."""
    import glob
    for pad in glob.glob(os.path.join(hj.DOEL_MAP, f"{fid}_*_{jaar}.pdf")):
        try:
            if hj.keur(pad, jaar, naam, True) is None:
                return pad
        except Exception:
            continue
    return None


def vul(con, jaar: int, opnieuw: bool) -> None:
    con.execute(SCHEMA)
    # Wie staat nog niet op dit boekjaar? Fondsen zonder énige analyse horen er
    # ook bij, anders blijft de staart van kleine fondsen liggen.
    rijen = con.execute("""
        SELECT f.id, f.name, COALESCE(f.aum_euro_bn, 0)
        FROM funds f
        WHERE COALESCE(f.is_pensioenfonds, 1) = 1
          AND NOT EXISTS (SELECT 1 FROM fund_analysis a
                          WHERE a.fund_id = f.id AND a.fiscal_year >= ?)
        ORDER BY COALESCE(f.aum_euro_bn, 0) DESC""", (jaar,)).fetchall()

    nieuw = al_binnen = 0
    for fid, naam, _aum in rijen:
        cur = con.execute("SELECT status FROM ophaal_wachtrij WHERE fund_id=? AND jaar=?",
                          (fid, jaar)).fetchone()
        # Staat het verslag er al? Dat komt voor bij fondsen die eerder met de
        # hand zijn opgehaald. Opnieuw downloaden kost minuten browserwerk voor
        # een bestand dat al op schijf staat.
        bestaand = _bestaand_verslag(fid, jaar, naam)
        if cur is None:
            con.execute("""INSERT INTO ophaal_wachtrij (fund_id, jaar, status, pad, bijgewerkt)
                           VALUES (?,?,?,?,?)""",
                        (fid, jaar, "binnen" if bestaand else "open", bestaand, _nu()))
            nieuw += 1
            al_binnen += bool(bestaand)
        elif opnieuw and cur[0] in ("niet_gevonden", "afgekeurd"):
            con.execute("""UPDATE ophaal_wachtrij SET status='open', bijgewerkt=?
                           WHERE fund_id=? AND jaar=?""", (_nu(), fid, jaar))
            nieuw += 1
    con.commit()
    print(f"{len(rijen)} fondsen staan nog niet op boekjaar {jaar}; {nieuw} in de rij gezet"
          + (f", waarvan {al_binnen} met een verslag dat er al stond." if al_binnen else "."))

    # Voor die reeds aanwezige verslagen alsnog de passages uitsnijden.
    ontbreekt = con.execute(
        """SELECT fund_id, pad FROM ophaal_wachtrij w
           WHERE jaar=? AND status='binnen' AND pad IS NOT NULL""", (jaar,)).fetchall()
    os.makedirs(KERN_MAP, exist_ok=True)
    gemaakt = 0
    for fid, pad in ontbreekt:
        kern = os.path.join(KERN_MAP, f"{fid}_{jaar}.md")
        if os.path.exists(kern) or not pad or not os.path.exists(pad):
            continue
        naam = con.execute("SELECT name FROM funds WHERE id=?", (fid,)).fetchone()[0]
        try:
            with open(kern, "w") as f:
                f.write(snij_passages(pad, naam, jaar))
            gemaakt += 1
        except Exception as e:
            print(f"  passages mislukt voor {fid}: {type(e).__name__}")
    if gemaakt:
        print(f"{gemaakt} briefings uitgesneden naar data/interim/kern/.")


def markeer_geanalyseerd(con, jaar: int) -> int:
    """Wat inmiddels in fund_analysis staat, hoeft niet meer opgehaald."""
    n = con.execute("""UPDATE ophaal_wachtrij SET status='geanalyseerd', bijgewerkt=?
        WHERE jaar=? AND status<>'geanalyseerd' AND fund_id IN
              (SELECT fund_id FROM fund_analysis WHERE fiscal_year=?)""",
                    (_nu(), jaar, jaar)).rowcount
    con.commit()
    return n


def verwerk(con, jaar: int, minuten: int, maxpogingen: int) -> None:
    con.execute(SCHEMA)
    markeer_geanalyseerd(con, jaar)
    rij = con.execute("""
        SELECT w.fund_id, f.name, f.website
        FROM ophaal_wachtrij w JOIN funds f ON f.id = w.fund_id
        WHERE w.jaar=? AND w.status='open' AND w.pogingen < ?
        ORDER BY COALESCE(f.aum_euro_bn, 0) DESC""", (jaar, maxpogingen)).fetchall()
    if not rij:
        print(f"Wachtrij voor {jaar} is leeg.")
        return

    os.makedirs(KERN_MAP, exist_ok=True)
    os.makedirs(hj.DOEL_MAP, exist_ok=True)
    einde = time.monotonic() + minuten * 60
    print(f"{len(rij)} fondsen in de rij, tijdslot {minuten} minuten.\n", flush=True)

    from playwright.sync_api import sync_playwright
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(user_agent=hj.UA, locale="nl-NL")
    pg = ctx.new_page()

    tellers = {"binnen": 0, "niet_gevonden": 0, "afgekeurd": 0}
    try:
        for fid, naam, website in rij:
            if time.monotonic() > einde:
                print("\nTijdslot op — de rest blijft open staan.", flush=True)
                break

            kort = re.sub(r"[^A-Za-z0-9]+", "_", naam.split("(")[0].strip())[:24].strip("_")
            pad = os.path.join(hj.DOEL_MAP, f"{fid}_{kort}_{jaar}.pdf")
            url, data = hj.kies_url(con, fid, jaar), None

            if url:
                try:
                    req = urllib.request.Request(
                        url, headers={"User-Agent": hj.UA, "Accept": "application/pdf,*/*"})
                    with urllib.request.urlopen(req, timeout=120) as r:
                        data = r.read()
                except Exception:
                    data = None
            if data is None and website:
                try:
                    gevonden = hj.zoek_en_haal_via_site(pg, website, jaar)
                except Exception:
                    gevonden = None
                if gevonden:
                    url, data = gevonden

            if data is None:
                _zet(con, fid, jaar, "niet_gevonden", f"geen {jaar}-verslag gevonden", url)
                tellers["niet_gevonden"] += 1
                print(f"  {fid:>4} {naam[:32]:<33} niets gevonden", flush=True)
                continue

            with open(pad, "wb") as f:
                f.write(data)
            reden = hj.keur(pad, jaar, naam, hj.zelfde_domein(url, website))
            if reden:
                os.remove(pad)
                _zet(con, fid, jaar, "afgekeurd", reden, url)
                tellers["afgekeurd"] += 1
                print(f"  {fid:>4} {naam[:32]:<33} afgekeurd: {reden[:44]}", flush=True)
                continue

            kern = os.path.join(KERN_MAP, f"{fid}_{jaar}.md")
            try:
                with open(kern, "w") as f:
                    f.write(snij_passages(pad, naam, jaar))
            except Exception as e:
                _zet(con, fid, jaar, "afgekeurd", f"passages mislukt ({type(e).__name__})", url, pad)
                tellers["afgekeurd"] += 1
                continue

            _zet(con, fid, jaar, "binnen", None, url, pad)
            tellers["binnen"] += 1
            print(f"  {fid:>4} {naam[:32]:<33} binnen  {os.path.getsize(pad)//1024} kB", flush=True)
    finally:
        browser.close()
        pw.stop()

    print(f"\n{tellers['binnen']} binnen, {tellers['afgekeurd']} afgekeurd, "
          f"{tellers['niet_gevonden']} niet gevonden.", flush=True)
    stand(con, jaar)


def stand(con, jaar: int | None = None) -> None:
    con.execute(SCHEMA)
    waar, param = ("WHERE jaar=?", (jaar,)) if jaar else ("", ())
    rijen = con.execute(
        f"SELECT jaar, status, COUNT(*) FROM ophaal_wachtrij {waar} GROUP BY 1,2 ORDER BY 1 DESC, 3 DESC",
        param).fetchall()
    if not rijen:
        print("Wachtrij is nog niet gevuld.")
        return
    print("\nstand van de wachtrij")
    for jr, status, n in rijen:
        print(f"  {jr}  {status:<14} {n:>3}")

    klaar = con.execute(
        f"""SELECT w.fund_id, f.name FROM ophaal_wachtrij w JOIN funds f ON f.id=w.fund_id
            {waar and waar + ' AND' or 'WHERE'} w.status='binnen'
            ORDER BY COALESCE(f.aum_euro_bn,0) DESC""", param).fetchall()
    if klaar:
        print(f"\nklaar om uit te schrijven ({len(klaar)}):")
        for fid, naam in klaar:
            print(f"  {fid:>4}  {naam[:44]}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("vul", help="zet fondsen zonder analyse over dit jaar in de rij")
    p.add_argument("--jaar", type=int, default=2025)
    p.add_argument("--opnieuw", action="store_true",
                   help="ook eerder mislukte fondsen opnieuw proberen")

    p = sub.add_parser("verwerk", help="werk de rij af tot hij leeg is of de tijd op is")
    p.add_argument("--jaar", type=int, default=2025)
    p.add_argument("--minuten", type=int, default=45)
    p.add_argument("--pogingen", type=int, default=2, help="hoe vaak een fonds hoogstens geprobeerd wordt")

    p = sub.add_parser("stand", help="toon de voortgang")
    p.add_argument("--jaar", type=int)

    args = ap.parse_args()
    con = sqlite3.connect(DB_PATH, timeout=60)
    try:
        if args.cmd == "vul":
            vul(con, args.jaar, args.opnieuw)
            stand(con, args.jaar)
        elif args.cmd == "verwerk":
            verwerk(con, args.jaar, args.minuten, args.pogingen)
        else:
            stand(con, args.jaar)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
