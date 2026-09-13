# Ontbrekende jaarverslagen 2025 — werklijst

Stand 2026-09-13, gegenereerd met `scripts/utils_and_viz/maak_werklijst.py`. 26 pensioenfondsen hebben nog geen analyse over boekjaar 2025.

**Kolom "Laatste"** is het nieuwste boekjaar waarvan we wél een analyse hebben. Staat daar 2024, dan zijn we precies één jaar achter.

**Een verslag gevonden?** Zet hem neer als `data/annual_reports/<id>_<KorteNaam>_2025.pdf` en draai daarna `wachtrij.py vul --jaar 2025 --opnieuw`; die ziet het bestand staan, zet de status op `binnen` en snijdt de passages uit. Heb je alleen een URL, dan is `haal_jaarverslagen.py --jaar 2025 --via-site --fondsen <id>` sneller. Het uitschrijven van de analyse blijft handwerk.

## Open fondsen (10)

Hier zit de meeste waarde: fondsen die nog gewoon draaien en dus een jaarverslag horen te publiceren.

| ✓ | id | Fonds | AUM € mld | Laatste | Website | Waarom niet gevonden |
|---|---|---|---|---|---|---|
| ☐ | 102 | Hagee | 0,68 | geen | [link](https://www.hagee.nl/) | draagt boekjaar 2024, niet 2025 ({2024: 126, 2023: 4, 2026: 1, 2022: 1, 2025: 3}) |
| ☐ | 117 | Nederlandse Bisdommen | 0,55 | 2024 | [link](https://www.pnb.nl/bisdom/) | niets gevonden op de site |
| ☐ | 110 | KAS BANK | 0,34 | 2024 | [link](https://www.pensioenfondskasbank.nl/) | niets gevonden op de site |
| ☐ | 7 | Roeiers (SPRH - Rowers) | 0,19 | geen | [link](https://sprh.nl/) | niets gevonden op de site |
| ☐ | 101 | Gomacon | 0,01 | geen | [link](https://www.werkenaanonspensioen.nl/belangrijke-begrippen/overzicht-pensioenfondsen-en-pensioenverzekeraars) | niets gevonden op de site |
| ☐ | 66 | De Nationale APF | – | 2024 | [link](https://www.denationaleapf.nl/) | niets gevonden op de site |
| ☐ | 134 | Tobacon Offshore Marine Consultancy B.V. | – | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 224 | Kring E-DC (De Nationale) | – | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 225 | Kring G-Cargill (De Nationale) | – | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 226 | Kring H NN CDC (De Nationale) | – | geen | **ontbreekt** | niets gevonden op de site |

## Ingevaren fondsen (3)

| ✓ | id | Fonds | AUM € mld | Laatste | Website | Waarom niet gevonden |
|---|---|---|---|---|---|---|
| ☐ | 181 | Oak | 5,34 | 2024 | [link](https://www.oakpensioenfonds.nl/) | niets gevonden op de site |
| ☐ | 133 | TNO | 3,97 | 2024 | [link](https://www.pensioenfondstno.nl/) | niets gevonden op de site |
| ☐ | 136 | Vopak | 1,26 | 2024 | [link](https://pensioenfondsvopak-pensioen123.nl/pensioen123/) | niets gevonden op de site |

## Gesloten, opgeheven of in liquidatie (13)

Bij deze groep is het goed mogelijk dat er geen verslag over dit boekjaar meer komt. Loop ze pas na als de rest af is.

| ✓ | id | Fonds | AUM € mld | Laatste | Website | Waarom niet gevonden |
|---|---|---|---|---|---|---|
| ☐ | 233 | Nedlloyd | 0,96 | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 232 | Honeywell | 0,70 | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 235 | YARA Nederland | 0,65 | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 131 | TDV | 0,64 | 2024 | [link](https://pensioenfonds-tdv.nl/) | draagt boekjaar 2024, niet 2025 ({2024: 93, 2023: 4, 2025: 4}) |
| ☐ | 207 | Pensioenfonds British American Tobacco | 0,63 | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 208 | Pensioenfonds Trespa | 0,19 | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 114 | Mercer | 0,15 | 2023 | [link](https://www.pensioenfondsmercer.nl/) | niets gevonden op de site |
| ☐ | 236 | Coram | 0,07 | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 234 | Pensura | 0,03 | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 237 | Calpam | – | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 206 | Pensioenfonds Ernst & Young | – | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 231 | Tandartsen en Tandarts-Specialisten | – | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 241 | Grolsche Bierbrouwerij | – | geen | **ontbreekt** | niets gevonden op de site |

## Wat we onderweg tegenkwamen

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

## Sites die geen enkele scraper binnenlaten

Een aparte categorie: fondsen achter bot-bescherming van het Imperva-type. Daar
geeft de site 403 op een kale request, op een echte browsersessie én op een
losse fetcher, en bij De Nationale APF zelfs op het /media/-pad waar de PDFs
zelf staan. Vastgesteld bij Capgemini, Gasunie, pnb.nl (Nederlandse Bisdommen)
en denationaleapf.nl (het APF plus zijn kringen).

Daar helpt geen crawl-logica tegen. De enige route is het bestand met de hand
uit een gewone browser halen. Loopt de werklijst tegen de reden "site blokkeert
elke geautomatiseerde toegang" aan, probeer het dan niet opnieuw met de zoeker.

## Laatste redmiddel

Staat een verslag echt nergens, dan is het jaarrekeningdeel vaak wel bij DNB of
via de KvK te vinden. Voor ingevaren fondsen loont het om bij de nieuwe
uitvoerder te kijken.
