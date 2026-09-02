# De cijfers over 2025 in `funds` en `historical_metrics` deugen op veel plekken niet

Gevonden op 2 september 2026, terwijl de analyses over de nieuwe jaarverslagen
werden geschreven. Elf verslagen zijn regel voor regel naast de database gelegd.
Bij tien van de elf week er iets af, en vaak niet iets kleins.

Dit begon met één opmerking: de agent die het Cargill-verslag las, zag dat
`deelnemers_actief` daar 22.831 was terwijl het verslag 745 zegt. 22.831 is de
premie in duizenden euro's, die één regel verderop in dezelfde kerncijfertabel
staat.

## Drie soorten fouten

**Een getal uit de verkeerde kolom of de verkeerde regel.**

| fonds | veld | database | verslag |
|---|---|---:|---:|
| Cargill | deelnemers_actief | 22.831 | 745 |
| MSD | deelnemers_pensioengerechtigd | 31 | 2.387 |
| Foodservice | belegd vermogen | 0,331 mrd | 1,85 mrd |
| Foodservice | beleggingsrendement | +1,3% | −11,0% |
| Foodservice | beleidsdekkingsgraad | 112,0% | 126,1% |

Bij Foodservice zit de fout ook in de `funds`-tabel zelf: `aum_euro_bn` staat op
2,006, wat het belegd vermogen van 2024 in duizenden euro's is, gelezen als
miljarden. En `transactiekosten_pct` staat op 1,695, wat geen percentage is maar
een bedrag in duizenden euro's.

**Het jaar ernaast.** De rij van 2025 draagt bij Forbo de kolom van 2024
(920/792/829 in plaats van 898/778/873). Rockwool heeft in `funds` een
deelnemerstotaal van 3.219, en dat is de stand van 2022.

**Hetzelfde getal over meerdere kolommen uitgesmeerd.** Zes rijen over 2025
hebben `deelnemers_slapers` exact gelijk aan `deelnemers_pensioengerechtigd`.
Bij Molenaars zijn alle drie de kolommen 2.048. Over 2024 komt dit patroon één
keer voor, over 2025 zes keer.

| id | fonds | actief | slapers | gepensioneerd |
|---:|---|---:|---:|---:|
| 46 | IKEA | 16.833 | 817 | 817 |
| 80 | Avery Dennison | 10.066 | 473 | 473 |
| 26 | Molenaars | 2.048 | 2.048 | 2.048 |
| 74 | Alliance | 1.182 | 680 | 680 |
| 1 | Dierenartsen | — | 4.725 | 4.725 |
| 136 | Vopak | — | 400 | 400 |

Daarnaast springen er drie zonder dat een fonds zo kan groeien of krimpen:
Schilders van 34.465 naar 103.000 actieven, IKEA van 7.387 naar 16.833, Zuivel
van 9.950 naar 1.970. PGB staat op 22 gepensioneerden. Loodsen, een klein
beroepsfonds, staat op 17.109 actieven met 10 slapers en 20 gepensioneerden.

## Losse vondsten die geen getal zijn

Bij het APF Het Nederlandse Pensioenfonds staat als uitvoerder "bevestigd door
externe specialisten". Dat is geen uitvoerder maar een stuk zin uit het verslag.
Het vermogen staat er op nul. De `funds`-rij draagt cijfers die in het verslag
van 1.131 pagina's nergens voorkomen. En `apf_profiel` telt twaalf kringen
waarvan vijf flexibel, terwijl het verslag er dertien telt waarvan zeven
solidair.

Bij Rockwool staat in het omschrijvingsveld "877.542 deelnemers", bij een fonds
met drieduizend mensen.

## Waarom de bestaande controle dit niet ziet

`check_data_quality.py` draait bij elke scrape en heeft veertien controles,
waaronder "Vermogen en deelnemers in de jaarreeks passen niet bij elkaar" en
"Waarden buiten elk redelijk bereik in de jaarreeks". Beide meldden niets.

De reden is dat elke controle naar één rij of één verhouding kijkt, en de meeste
van deze fouten zijn plausibel op zichzelf. 817 slapers is een normaal getal.
0,331 miljard is een normaal getal. Wat niet normaal is, is dat 817 twee keer
naast elkaar staat, of dat een fonds in één jaar van 2,0 naar 0,331 miljard gaat.

Wat ontbreekt zijn drie toetsen: twee deelnemerskolommen die exact gelijk zijn,
een sprong tussen twee opeenvolgende jaren die geen fonds maakt, en een veld dat
in `funds` niet strookt met de nieuwste rij in `historical_metrics`.

DNB kan hier niet als tegenproef dienen. `dnb_quarterly_metrics` bevat alleen
financiële grootheden en geen deelnemersaantallen.

## Wat ik voorstel

**Nu te doen, want het is eenduidig.** Voor de elf fondsen waarvan het verslag
op schijf staat, zijn de juiste cijfers nu bekend en per veld met een
paginanummer onderbouwd. Die kunnen weg, mits per fonds nagelopen.

**Voorleggen.** Voor de acht overige fondsen met een verdachte deelnemersrij
staat geen verslag op schijf. Leegmaken is beter dan laten staan, want een leeg
veld toont de grafiek als een gat en een fout veld toont hij als een feit. Maar
dat is een keuze van de eigenaar en niet van mij.

**Daarna.** De kwaliteitscontrole verdient de drie toetsen hierboven, anders
komt dit bij de volgende ronde gewoon terug.
