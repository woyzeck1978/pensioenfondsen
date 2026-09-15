# De cijfers over 2025 in `funds` en `historical_metrics` deugden op veel plekken niet

Gevonden op 2 september 2026, terwijl de analyses over de nieuwe jaarverslagen
werden geschreven. Elf verslagen zijn regel voor regel naast de database gelegd.
Bij tien van de elf week er iets af, en vaak niet iets kleins.

Dit begon met één opmerking: de agent die het Cargill-verslag las, zag dat
`deelnemers_actief` daar 22.831 was terwijl het verslag 745 zegt. 22.831 is de
premie in duizenden euro's, die één regel verderop in dezelfde kerncijfertabel
staat.

**Stand 15 september 2026: gecorrigeerd.** Zie *De controle van september*
onderaan. Wat hieronder staat, is de oorspronkelijke vondst.

## Drie soorten fouten

**Een getal uit de verkeerde kolom of de verkeerde regel.**

| fonds | veld | database | verslag |
|---|---|---:|---:|
| Cargill | deelnemers_actief | 22.831 | 745 |
| MSD | deelnemers_pensioengerechtigd | 31 | 2.387 |
| Foodservice | belegd vermogen | 0,331 mrd | 1,85 mrd |
| Foodservice | beleggingsrendement | +1,3% | −11,0% |
| Foodservice | beleidsdekkingsgraad | 112,0% | 126,1% |

Bij Foodservice zat de fout ook in de `funds`-tabel zelf: `aum_euro_bn` stond op
2,006, wat het belegd vermogen van 2024 in duizenden euro's is, gelezen als
miljarden. En `transactiekosten_pct` staat op 1,695, wat geen percentage is maar
een bedrag in duizenden euro's.

**Het jaar ernaast.** De rij van 2025 droeg bij Forbo de kolom van 2024
(920/792/829 in plaats van 898/778/873). Rockwool had in `funds` een
deelnemerstotaal van 3.219, en dat is de stand van 2022.

**Hetzelfde getal over meerdere kolommen uitgesmeerd.** Zes rijen over 2025
hadden `deelnemers_slapers` exact gelijk aan `deelnemers_pensioengerechtigd`.
Bij Molenaars waren alle drie de kolommen 2.048.

| id | fonds | actief | slapers | gepensioneerd |
|---:|---|---:|---:|---:|
| 46 | IKEA | 16.833 | 817 | 817 |
| 80 | Avery Dennison | 10.066 | 473 | 473 |
| 26 | Molenaars | 2.048 | 2.048 | 2.048 |
| 74 | Alliance | 1.182 | 680 | 680 |
| 1 | Dierenartsen | — | 4.725 | 4.725 |
| 136 | Vopak | — | 400 | 400 |

Daarnaast sprongen er drie zonder dat een fonds zo kan groeien of krimpen:
Schilders van 34.465 naar 103.000 actieven, IKEA van 7.387 naar 16.833, Zuivel
van 9.950 naar 1.970. PGB stond op 22 gepensioneerden. Loodsen, een klein
beroepsfonds, stond op 17.109 actieven met 10 slapers en 20 gepensioneerden.

## Losse vondsten die geen getal zijn

Bij het APF Het Nederlandse Pensioenfonds staat als uitvoerder "bevestigd door
externe specialisten". Dat is geen uitvoerder maar een stuk zin uit het verslag.
En `apf_profiel` telt twaalf kringen waarvan vijf flexibel, terwijl het verslag
er dertien telt waarvan zeven solidair. *Nog niet opgelost.*

Bij Rockwool staat in het omschrijvingsveld "877.542 deelnemers", bij een fonds
met drieduizend mensen. *Nog niet opgelost.*

## Waarom de bestaande controle dit niet zag

Elke controle in `check_data_quality.py` keek naar één rij of één verhouding, en
de meeste van deze fouten zijn plausibel op zichzelf. 817 slapers is een normaal
getal. Wat niet normaal is, is dat 817 twee keer naast elkaar staat, of dat een
fonds in één jaar van 2,0 naar 0,331 miljard gaat. Daarom zijn er drie toetsen
bijgekomen: twee deelnemerskolommen in de jaarreeks die exact gelijk zijn, een
sprong van meer dan factor 2,5 tussen twee opeenvolgende jaren, en een veld in
`funds` dat meer dan een kwart afwijkt van de nieuwste rij in `historical_metrics`.

## De controle van september

Van 173 van de 176 fondsen met een analyse over 2025 is het jaarverslag opnieuw
binnengehaald en zijn de kerncijfers uitgelezen: rendement, beleids-, actuele,
reële en vereiste dekkingsgraad, belegd vermogen, de vier deelnemersaantallen en
de renteafdekking — elk met paginanummer en de letterlijke vindplaats. De drie
zonder eigen cijfers: J&J (geen openbaar verslag over 2025; het verslag van de
Pensioenraad staat achter een inlog) en de APF-koepels Het Nederlandse
Pensioenfonds en Unilever APF, die hun kerncijfers alleen per kring rapporteren
— die kringen zijn wel gecontroleerd.

Een wijziging is alleen doorgevoerd als drie dingen klopten:

1. de eerste lezing (Claude-agents; voor PGB, SPW, APF en Rijn- en Binnenvaart
   Gemini) gaf de waarde met pagina;
2. een tweede, onafhankelijke lezing door Gemini tegen dezelfde pagina's gaf
   `voorstel_klopt`;
3. het getal staat letterlijk op die pagina, in Nederlandse of Engelse notatie.

Uitkomst:

| | aantal |
|---|---:|
| waarden over 2025 vervangen | 337, bij 106 fondsen |
| lege velden over 2025 gevuld | 701 |
| vergelijkende cijfers over 2024 gevuld | 708 |
| deelnemersvelden leeggemaakt (leesfoutpatroon, niet te bevestigen) | 10 |
| publicatiedatum (vaststelling door het bestuur) ingevuld | 122 |
| `funds`: vermogen gelijkgetrokken met FY2025 | 19 fondsen |
| `funds`: deelnemersaantallen gelijkgetrokken met bevestigde FY2025-waarden | 521 velden, 149 fondsen |
| `funds`: onbevestigde deelnemersvelden in een kapotte rij leeggemaakt | 3 |

Niet doorgevoerd en dus nog open:

- **Renteafdekking en belegd vermogen, 152 verschillen.** Verslagen noemen vaak
  alleen een strategisch afdekkingspercentage, of meten de afdekking op de
  dekkingsgraad in plaats van op de verplichtingen. Bij het vermogen is het
  verslagcijfer meestal het bruto balanstotaal (ABP 559,7 mrd) waar de database
  het netto belegd vermogen heeft (531,8 mrd). Een definitiekeuze, geen
  leesfout; de database is niet gewijzigd.
- **Afwijkingen in de 2024-rijen, 190.** De cijferreeks over 2024 komt uit de
  verslagen over 2024; het vergelijkende cijfer in het verslag over 2025 is
  alleen gebruikt om lege velden te vullen, niet om bestaande te vervangen.
- **Twaalf items** waarover de tweede lezing geen uitsluitsel gaf, twee waarbij
  zij de database gelijk gaf (waaronder het rendement van Verloskundigen, waar
  de eerste lezing −10,1% las en het verslag −5,4% zegt) en drie waarvan het
  getal niet letterlijk op de pagina te vinden was.
- **Deelnemers die niet optellen tot het totaal**, bij zestien fondsen. Bij
  twaalf gaat het om 1 tot 5% en telt het verslag in het totaal een groep mee
  die niet in de drie kolommen past (arbeidsongeschikten). Bij HAL en Rabobank
  geeft het verslag geen totaal aantal pensioengerechtigden, bij Loodsen alleen
  deelaantallen, en bij PGB is het totaal (353.494) lager dan de som van de
  onderdelen — wat "totaal" daar omvat, is onduidelijk.
- **Sprongen die echt zijn**: Rijn- en Binnenvaart en Staples droegen in 2025 hun
  rechten over, Fysiotherapeuten voer per 1 oktober 2025 in. Bij Foodservice
  (2024: 0,308 mrd) en StiPP is de 2024-rij de verdachte, niet die van 2025.

Alle logs staan in `docs/verslagcontrole_2025/`: `correctielog_2025.csv` (elk
verschil), `toegepast_2025.csv` (wat is doorgevoerd), `ter_beoordeling_2025.csv`
(wat niet), de leeggemaakte velden, per fonds de uitgelezen cijfers in
`verslagcijfers/<fund_id>.json`, en de scripts waarmee het is gedaan.
