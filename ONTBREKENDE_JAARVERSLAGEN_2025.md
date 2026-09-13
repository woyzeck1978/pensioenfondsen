# Ontbrekende jaarverslagen 2025 — werklijst

Stand 2026-09-13. 52 van de 192 pensioenfondsen hebben nog geen analyse over boekjaar
2025. Deze lijst is bedoeld om met de hand langs te lopen: vink af wat je hebt
gevonden en zet de PDF-URL erbij.

**Kolom "Laatste"** is het nieuwste boekjaar waarvan we wél een analyse hebben.
Staat daar 2024, dan zijn we precies één jaar achter. Staat er iets ouders, dan
loopt het langer mis en is het fonds waarschijnlijk lastiger vindbaar.

**Een verslag gevonden?** Zet hem neer als
`data/annual_reports/<id>_<KorteNaam>_2025.pdf` en draai daarna:

```bash
cd ~/pensioenfondsen-app
.venv/bin/python3 scripts/automation/wachtrij.py vul --jaar 2025 --opnieuw
```

`vul` ziet een bestand dat er al staat, zet de status op `binnen` en snijdt de
passages uit naar `data/interim/kern/<id>_2025.md`. Daarna is het uitschrijven
van de analyse nog handwerk.

Heb je alleen een URL, dan is dit sneller:

```bash
.venv/bin/python3 scripts/data_collection/haal_jaarverslagen.py \
    --jaar 2025 --via-site --fondsen <id>
```

Let op de keuring: die wijst een verslag af als het een ander boekjaar draagt,
als de fondsnaam er nergens in staat, of als het geen pensioenfondsverslag is.
Dat laatste ving het engagementrapport dat onder Detailhandel binnenkwam.

## Open fondsen (31)

Hier zit de meeste waarde: fondsen die nog gewoon draaien en dus een
jaarverslag horen te publiceren.

| ✓ | id | Fonds | AUM € mld | Laatste | Website | Waarom niet gevonden |
|---|---|---|---|---|---|---|
| ☐ | 17 | Detailhandel (Retail) | 32,31 | 2024 | [link](https://pensioenfondsdetailhandel.nl/) | engagementrapport van het Dutch Engagement Network, geen jaarverslag |
| ☐ | 106 | Hoogovens | 10,41 | 2024 | [link](https://deelnemerportaal.pfhoogovens.nl/) | niets gevonden op de site |
| ☐ | 45 | IBM (SPIN) / IBM Nederland | 3,50 | 2024 | [link](https://spin.metpensioen.nl/) | niets gevonden op de site |
| ☐ | 126 | SABIC | 2,92 | 2024 | [link](https://spf-pensioenen.nl/nl) | draagt boekjaar 2022, niet 2025 ({2022: 102, 2015: 1, 2021: 5, 2023: 3}) |
| ☐ | 97 | Gasunie | 2,31 | 2024 | [link](https://www.pensioenfondsgasunie.nl/) | niets gevonden op de site |
| ☐ | 6 | Openbare Apothekers (SPOA - Public Pharmacists) | 1,88 | geen | [link](https://www.spoa.nl/) | niets gevonden op de site |
| ☐ | 89 | DHL Nederland | 1,24 | 2024 | [link](https://www.pfdhl.nl/) | niets gevonden op de site |
| ☐ | 50 | Mediahuis Nederland (Mhpf) | 1,24 | geen | [link](https://www.mhpf.nl/) | fondsnaam komt niet voor in het document (gezocht op mediahuis) |
| ☐ | 83 | Capgemini / Capgemini Nederland | 1,05 | 2024 | [link](https://www.pensioenfondscg.nl/) | niets gevonden op de site |
| ☐ | 183 | Pensioenkring 2 (Stap) | 0,88 | 2024 | [link](https://www.stappensioen.nl/pensioenkring-2/pensioenkring) | niets gevonden op de site |
| ☐ | 172 | Kring Forward (Unilever) | 0,71 | geen | [link](https://www.unileverpensioenfonds.nl/) | niets gevonden op de site |
| ☐ | 102 | Hagee | 0,68 | geen | [link](https://www.hagee.nl/) | niets gevonden op de site |
| ☐ | 131 | TDV | 0,64 | 2024 | [link](https://pensioenfonds-tdv.nl/) | draagt boekjaar 2024, niet 2025 ({2024: 93, 2023: 4, 2025: 4}) |
| ☐ | 93 | Essity | 0,62 | 2024 | [link](https://www.mijnessitypensioen.nl/) | niets gevonden op de site |
| ☐ | 79 | Avebe | 0,57 | 2024 | [link](https://pensioenfondsavebe.nl/) | niets gevonden op de site |
| ☐ | 117 | Nederlandse Bisdommen | 0,55 | 2024 | [link](https://www.pnb.nl/bisdom/) | niets gevonden op de site |
| ☐ | 158 | IFF | 0,47 | geen | [link](https://iffpensioenfonds.nl/) | niets gevonden op de site |
| ☐ | 81 | Brocacef | 0,35 | 2024 | [link](https://www.brocacefpensioenfonds.nl/) | draagt boekjaar 2024, niet 2025 ({2024: 19, 2021: 1, 2025: 2, 2023: 1}) |
| ☐ | 127 | Sagittarius | 0,34 | 2024 | [link](https://pensioenfonds-sagittarius.nl/) | draagt boekjaar 2024, niet 2025 ({2024: 10, 2023: 2, 2016: 2, 2025: 1}) |
| ☐ | 110 | KAS BANK | 0,34 | 2024 | [link](https://www.pensioenfondskasbank.nl/) | niets gevonden op de site |
| ☐ | 130 | Sportfondsen | 0,20 | 2024 | [link](https://www.sportfondsenpensioenfonds.nl/) | draagt boekjaar 2024, niet 2025 ({2024: 23, 2023: 1, 2025: 2}) |
| ☐ | 7 | Roeiers (SPRH - Rowers) | 0,19 | geen | [link](https://sprh.nl/) | niets gevonden op de site |
| ☐ | 101 | Gomacon | 0,01 | geen | [link](https://www.werkenaanonspensioen.nl/belangrijke-begrippen/overzicht-pensioenfondsen-en-pensioenverzekeraars) | niets gevonden op de site |
| ☐ | 66 | De Nationale APF | – | 2024 | [link](https://www.denationaleapf.nl/) | niets gevonden op de site |
| ☐ | 68 | Unilever APF | – | 2024 | [link](https://www.unileverpensioenfonds.nl/) | niets gevonden op de site |
| ☐ | 134 | Tobacon Offshore Marine Consultancy B.V. | – | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 224 | Kring E-DC (De Nationale) | – | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 225 | Kring G-Cargill (De Nationale) | – | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 226 | Kring H NN CDC (De Nationale) | – | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 229 | Kring Flexibele Regeling (HNPF) | – | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 230 | Pensioenkring Cargill (HNPF) | – | geen | **ontbreekt** | niets gevonden op de site |

## Ingevaren fondsen (5)

| ✓ | id | Fonds | AUM € mld | Laatste | Website | Waarom niet gevonden |
|---|---|---|---|---|---|---|
| ☐ | 181 | Oak | 5,34 | 2024 | [link](https://www.oakpensioenfonds.nl/) | niets gevonden op de site |
| ☐ | 25 | MITT (Fashion, Interior, Carpet, and Textile Industry) | 4,15 | 2024 | [link](https://www.pensioenfondsmitt.nl/) | niets gevonden op de site |
| ☐ | 133 | TNO | 3,97 | 2024 | [link](https://www.pensioenfondstno.nl/) | draagt boekjaar 2024, niet 2025 ({2024: 177, 2025: 3, 2023: 9, 2021: 3, 2026: 1, 2022: 2}) |
| ☐ | 136 | Vopak | 1,26 | 2024 | [link](https://pensioenfondsvopak-pensioen123.nl/pensioen123/) | niets gevonden op de site |
| ☐ | 43 | Zuivel- en aanverwante industrie / BPZ (Dairy Industry) | 0,83 | 2024 | [link](https://www.pensioenfondszuivel.nl/) | draagt boekjaar 2024, niet 2025 ({2023: 2, 2024: 13, 2025: 1}) |

## Gesloten, opgeheven of in liquidatie (16)

Bij deze groep is het goed mogelijk dat er geen verslag over 2025 meer komt.
Loop ze pas na als de rest af is.

| ✓ | id | Fonds | AUM € mld | Laatste | Website | Waarom niet gevonden |
|---|---|---|---|---|---|---|
| ☐ | 82 | Campina | 1,25 | geen | [link](https://www.pensioenfondscampina.nl/over-ons/financiele-situatie/) | niets gevonden op de site |
| ☐ | 233 | Nedlloyd | 0,96 | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 193 | Kring Van Lanschot (HNPF) | 0,95 | 2024 | [link](https://pensioenfondsvanlanschot.nl/) | niets gevonden op de site |
| ☐ | 232 | Honeywell | 0,70 | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 235 | YARA Nederland | 0,65 | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 207 | Pensioenfonds British American Tobacco | 0,63 | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 208 | Pensioenfonds Trespa | 0,19 | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 103 | HAL | 0,17 | 2024 | [link](https://pensioenfondshal.nl/downloads/jaarverslag/) | draagt boekjaar 2019, niet 2025 ({2019: 18, 2013: 1, 2018: 10, 2020: 1}) |
| ☐ | 77 | AT&T Nederland | 0,16 | 2024 | [link](https://att-pensioenfonds.nl/) | draagt boekjaar 2024, niet 2025 ({2024: 12, 2023: 2, 2014: 2, 2025: 1}) |
| ☐ | 114 | Mercer | 0,15 | 2023 | [link](https://www.pensioenfondsmercer.nl/) | niets gevonden op de site |
| ☐ | 236 | Coram | 0,07 | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 234 | Pensura | 0,03 | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 237 | Calpam | – | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 206 | Pensioenfonds Ernst & Young | – | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 231 | Tandartsen en Tandarts-Specialisten | – | geen | **ontbreekt** | niets gevonden op de site |
| ☐ | 241 | Grolsche Bierbrouwerij | – | geen | **ontbreekt** | niets gevonden op de site |

## Bekende obstakels

- **Hoogovens (106)** — de website in de tabel wijst naar het deelnemerportaal.
  De echte site is `www.pfhoogovens.nl`, maar de documentenpagina daar laadt via
  JavaScript en levert ook gerenderd nul PDF-links op; het verslag zit
  waarschijnlijk achter een login. Corrigeer sowieso `funds.website`.
- **Detailhandel (17)** — de zoeker vond hier "Voortgang Thematische Dialogen
  juli-december 2025" van het Dutch Engagement Network en hield dat voor het
  jaarverslag. Dat is sinds deze week afgevangen. Hun eigen verslagenpagina
  (`/onze-organisatie/publicaties/jaarverslag`) eindigde bij 2024.
- **SABIC (126)** — `spf-pensioenen.nl` heeft verslagen tot en met 2023 op
  `/client/spfpensioen/upload/jaarverslagen/nl/`. Drie runs pakten daar een
  willekeurig oud verslag; 2025 stond er niet bij.
- **Oak (181)** — publiceert volgens de eerdere scrape alleen een verkort
  jaarverslag, en dat filtert de zoeker bewust weg.
- **Gomacon (101)** — geen eigen fondssite in de tabel.
- **Mediahuis (50)** — het nieuwste stuk op de documentenpagina is het
  MVB-verslag, niet het jaarverslag.

## Twee clusters die je apart kunt aanpakken

**Zes fondsen hebben helemaal geen website in de tabel** (134 Tobacon, 224 Kring
E-DC, 225 Kring G-Cargill, 226 Kring H NN CDC, 229 Kring Flexibele Regeling,
230 Pensioenkring Cargill). Het zijn kringen binnen een algemeen pensioenfonds;
hun deelverslagen staan op de site van het APF zelf — De Nationale APF
(`denationaleapf.nl`) respectievelijk Het Nederlandse Pensioenfonds
(`hetnederlandsepensioenfonds.nl`). Zonder `funds.website` komt de zoeker daar
nooit, dus die kolom invullen is hier de eerste stap en niet het zoeken zelf.
Let op dat de keuring bij een kring eist dat de kringnaam op de omslag staat:
het koepelverslag noemt álle kringen en wordt daarom terecht geweigerd.

**Vier fondsen leveren aantoonbaar hun verslag over 2024** (131 TDV, 81 Brocacef,
127 Sagittarius, 130 Sportfondsen). De zoeker vindt daar keurig het nieuwste
stuk dat er staat, en dat is 2024. Dat is dus geen zoekfout maar een fonds dat
nog niet heeft gepubliceerd. Zet die onderaan je lijst en controleer over een
paar maanden opnieuw.

## Laatste redmiddel

Staat een verslag echt nergens op de fondssite, dan is het jaarrekeningdeel vaak
wel bij DNB of via de KvK te vinden. Voor ingevaren fondsen loont het om bij de
nieuwe uitvoerder te kijken: NN CDC's verslag over 2025 verscheen onder de eigen
naam, maar de opvolger publiceert voortaan bij De Nationale APF.
