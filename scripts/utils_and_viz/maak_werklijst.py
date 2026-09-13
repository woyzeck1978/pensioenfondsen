# -*- coding: utf-8 -*-
"""Werklijst van de fondsen zonder analyse over een boekjaar.

Bedoeld om met de hand langs te lopen als de zoeker is uitgeput. De lijst zelf
komt uit de database, de toelichting eronder uit wat er tijdens het zoeken is
gebleken -- houd die bij, want die kennis zit verder nergens.

  python3 scripts/utils_and_viz/maak_werklijst.py --jaar 2025
"""
from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import date

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

TOELICHTING = """## Wat we onderweg tegenkwamen

Een verslag verstopt zich op meer manieren dan je zou denken. Vijf fondsen die
hier als "niet gevonden" stonden bleken het gewoon online te hebben, en elk
daarvan legde een eigen blinde vlek bloot. Alle vijf zijn inmiddels in de
zoeker gerepareerd, maar het loont om ze te kennen als je met de hand zoekt:

- **Het jaartal hoeft niet in de bestandsnaam te staan.** Detailhandel
  publiceert zijn verslag als `Jaarverslag_Pensioenfonds_Detailhandel_Spreads.pdf`
  en zet 2025 alleen in de linktekst.
- **Een document hoeft niet op `.pdf` te eindigen.** Hoogovens serveert het op
  `/meer-informatie/documenten/jaarverslag-2025/`, een download-endpoint.
- **Het hoeft niet op het eigen domein te staan.** SABIC hangt zijn verslag op
  `decisiontool.nl`, Gasunie op het subdomein `publicaties.`.
- **Twee jaartallen in een regel is normaal.** "Jaarverslag SPF 2025 (juli 2026)"
  -- het tweede getal is de publicatiemaand, niet het boekjaar.
- **De site in de tabel kan de verkeerde zijn.** Bij Hoogovens stond het
  deelnemerportaal, bij SPIN een oud adres. Controleer dat eerst.

Verder: **Gasunie blokkeert een headless browser met 403.** Daar helpt geen
crawl-logica tegen; zulke sites blijven handwerk.

## Twee clusters die je apart kunt aanpakken

**Fondsen zonder website in de tabel** zijn kringen binnen een algemeen
pensioenfonds; hun deelverslagen staan op de site van het APF zelf -- De
Nationale APF (`denationaleapf.nl`) of Het Nederlandse Pensioenfonds
(`hetnederlandsepensioenfonds.nl`). Zonder `funds.website` komt de zoeker daar
nooit, dus die kolom invullen is hier de eerste stap. Let op dat de keuring bij
een kring eist dat de kringnaam op de omslag staat: het koepelverslag noemt alle
kringen en wordt daarom terecht geweigerd.

**Lees "draagt boekjaar 2024" niet als "nog niet gepubliceerd".** Die melding
zegt alleen dat het nieuwste verslag dát de zoeker kon bereiken over 2024 ging.
Sagittarius, Sportfondsen en AT&T stonden er alle drie zo bij en hadden hun
verslag over 2025 gewoon online; zodra de zoeker hun echte documentenpagina
opende, stond het er. Alleen bij Hagee is met de hand vastgesteld dat er
werkelijk nog niets is. Wil je zekerheid, dan moet je de documentenpagina zelf
bekijken -- de melding zelf draagt die zekerheid niet.

## Laatste redmiddel

Staat een verslag echt nergens, dan is het jaarrekeningdeel vaak wel bij DNB of
via de KvK te vinden. Voor ingevaren fondsen loont het om bij de nieuwe
uitvoerder te kijken.
"""


def tabel(groep: list[dict]) -> str:
    uit = ["| ✓ | id | Fonds | AUM € mld | Laatste | Website | Waarom niet gevonden |",
           "|---|---|---|---|---|---|---|"]
    for r in groep:
        aum = ("%.2f" % r["aum"]).replace(".", ",") if r["aum"] else "–"
        site = "[link](%s)" % r["site"] if r["site"] else "**ontbreekt**"
        reden = r["reden"] or "–"
        if reden == "geen 2025-verslag gevonden" or reden.startswith("geen "):
            reden = "niets gevonden op de site"
        uit.append("| ☐ | %d | %s | %s | %s | %s | %s |"
                   % (r["id"], r["naam"], aum, r["laatst"] or "geen", site, reden))
    return "\n".join(uit)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jaar", type=int, default=2025)
    args = ap.parse_args()
    jaar = args.jaar

    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    rijen = [dict(id=r[0], naam=r[1], aum=r[2] or 0, status=r[3], site=r[4] or "",
                  reden=r[5] or "", laatst=r[6] or 0)
             for r in con.execute("""
        SELECT f.id, f.name, ROUND(COALESCE(f.aum_euro_bn,0),2), f.status,
               COALESCE(f.website,''), COALESCE(w.reden,''),
               COALESCE((SELECT MAX(a.fiscal_year) FROM fund_analysis a
                         WHERE a.fund_id=f.id),0)
        FROM funds f LEFT JOIN ophaal_wachtrij w ON w.fund_id=f.id AND w.jaar=?
        WHERE COALESCE(f.is_pensioenfonds,1)=1
          AND NOT EXISTS (SELECT 1 FROM fund_analysis a
                          WHERE a.fund_id=f.id AND a.fiscal_year=?)
        ORDER BY COALESCE(f.aum_euro_bn,0) DESC""", (jaar, jaar))]
    con.close()

    groepen = [("Open fondsen", [r for r in rijen if r["status"] == "Open"],
                "Hier zit de meeste waarde: fondsen die nog gewoon draaien en dus een "
                "jaarverslag horen te publiceren."),
               ("Ingevaren fondsen", [r for r in rijen if r["status"] == "Ingevaren"], ""),
               ("Gesloten, opgeheven of in liquidatie",
                [r for r in rijen if r["status"] not in ("Open", "Ingevaren")],
                "Bij deze groep is het goed mogelijk dat er geen verslag over dit "
                "boekjaar meer komt. Loop ze pas na als de rest af is.")]

    doc = [f"# Ontbrekende jaarverslagen {jaar} — werklijst", "",
           f"Stand {date.today().isoformat()}, gegenereerd met "
           "`scripts/utils_and_viz/maak_werklijst.py`. "
           f"{len(rijen)} pensioenfondsen hebben nog geen analyse over boekjaar {jaar}.", "",
           "**Kolom \"Laatste\"** is het nieuwste boekjaar waarvan we wél een analyse "
           f"hebben. Staat daar {jaar - 1}, dan zijn we precies één jaar achter.", "",
           "**Een verslag gevonden?** Zet hem neer als "
           f"`data/annual_reports/<id>_<KorteNaam>_{jaar}.pdf` en draai daarna "
           f"`wachtrij.py vul --jaar {jaar} --opnieuw`; die ziet het bestand staan, zet "
           "de status op `binnen` en snijdt de passages uit. Heb je alleen een URL, dan "
           f"is `haal_jaarverslagen.py --jaar {jaar} --via-site --fondsen <id>` sneller. "
           "Het uitschrijven van de analyse blijft handwerk.", ""]
    for titel, groep, intro in groepen:
        doc += [f"## {titel} ({len(groep)})", ""]
        if intro:
            doc += [intro, ""]
        doc += [tabel(groep), ""]
    doc += [TOELICHTING]

    uit = os.path.join(BASE_DIR, f"ONTBREKENDE_JAARVERSLAGEN_{jaar}.md")
    open(uit, "w", encoding="utf-8").write("\n".join(doc))
    print("geschreven: %s" % uit)
    for titel, groep, _ in groepen:
        print("  %-38s %d" % (titel, len(groep)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
