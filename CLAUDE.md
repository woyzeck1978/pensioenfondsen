# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> **New here?** Read `ROADMAP.md` first — it's the project state at the
> end of the May 2026 sessions: what's been done, what's open, and
> concrete first steps for each open item. This file describes *how*
> to work on the code; ROADMAP describes *what* to work on next.

## Project purpose

Research dataset of Dutch pension funds (`pensioenfondsen`). The pipeline scrapes fund websites, downloads annual reports / `transitieplannen` PDFs, parses metrics out of them, and stores everything in a single SQLite database that drives a Streamlit dashboard and an Excel export. The repo is a personal research tool, not a deployed product — scripts are run ad-hoc and frequently rewritten per fund.

## Canonical data store

**`data/processed/pension_funds.db`** is the source of truth. Almost every script reads from and/or writes to it. Inspect schema before editing data:

```bash
sqlite3 data/processed/pension_funds.db ".tables"
sqlite3 data/processed/pension_funds.db ".schema funds"
```

Key tables and what they hold:
- `funds` — one row per pension fund. Wide table (~60 columns) covering AUM, dekkingsgraad variants, deelnemers breakdown, equity/fixed-income/real-estate/alternatives allocation, costs, SFDR/ESG, WTP transition fields (`wtp_transitie_datum`, `wtp_contract_type`, `wtp_invaren`), uitvoerder/fiduciair/intern_beheer, plus free-text `equity_strategy_notes`, `beleggingsmix`, `investment_beliefs`, `description`.
- `historical_metrics` — per-year time series (rendement, dekkingsgraden, indexatie, CPI, AUM, deelnemer counts) keyed on `fund_id, year`.
- `news_articles`, `scraped_documents`, `fund_people` — discovered URLs / content from website scraping. `scraped_documents.doc_type='document'` plus title heuristics (LIKE `%jaarverslag%`, `%esg%`, etc.) is how the dashboard distinguishes annual reports from ESG reports.
- `equity_strategies`, `equity_strategy`, `equity_portfolio_funds`, `equity_allocations_extracted`, `fund_esg_metrics`, `monthly_funding_ratios` — narrower derived tables.

Note: `init_db.py` at `scripts/db_management/init_db.py` only reflects the original 9-column `funds` schema. The live DB has been extended via many ad-hoc `ALTER TABLE` migrations across the `db_management/` scripts; do not regenerate the DB from `init_db.py`.

## Common commands

Dependencies are minimal — `requirements.txt` only pins `streamlit`, `pandas`, `plotly`. The actual scripts also use `playwright`, `pymupdf` (`fitz`), `pypdf`, `python-docx`, `xlsxwriter`, `requests`, `beautifulsoup4` — install as needed.

```bash
# Run the dashboard (resolves DB via __file__, can be launched from anywhere)
streamlit run scripts/utils_and_viz/dashboard.py

# Refresh the Excel export from the DB
python3 scripts/utils_and_viz/export_excel.py        # run from project root

# Run the full automated pipeline (uses absolute paths from update_master.py)
python3 scripts/update_master.py
```

## Working-directory convention (important)

Scripts in this repo are inconsistent about how they locate the DB:

- **Run from project root**: `sqlite3.connect('data/processed/pension_funds.db')` — most analysis/utility scripts.
- **Run from the script's own directory**: `sqlite3.connect('../../data/processed/pension_funds.db')` — many `data_collection/` and `db_management/` scripts.
- **Self-locating** (`os.path.dirname(__file__)` chain) — `dashboard.py`, `update_master.py`. These always work.
- **Hardcoded Windows path** — old scripts like `init_db.py`, `extract_funds.py` were written on a Windows machine and contain `c:\Users\WebkoWuite\...` paths. Patch the path before running on this Mac.

Before running any script, check the `sqlite3.connect(...)` line and `cd` accordingly, or fix the path. New scripts should use the self-locating pattern from `dashboard.py:24-25`.

## Pipeline shape (data_collection → document_parsing → db_management → analysis)

`scripts/` is organized by stage, but execution is not strictly linear — most scripts are written to fix or extend one fund/metric and re-run independently:

1. **`scripts/data_collection/`** — Playwright/requests scrapers that crawl fund websites, populate `scraped_documents`, download PDFs to `data/annual_reports/`, `data/historical_reports/`, `data/transitieplannen/`. Several specialised variants exist (`monitor_websites_concurrent.py`, `monitor_websites_playwright.py`, `scrape_indexation_and_wtp_urls_robust.py`) because individual fund sites break standard locators.
2. **`scripts/document_parsing/`** — PDF readers (PyMuPDF/`pypdf`) that extract specific metrics. Many files (`read_hoogovens_page_16.py`, `extract_apg_page_53.py`, `extract_abn_page_44.py`) hardcode page numbers / regex per fund because annual reports have no shared structure. When adding a new fund, expect to write a new parser rather than extending a generic one.
3. **`scripts/db_management/`** — writers that `UPDATE funds SET …` from parsed values, plus `ALTER TABLE` migrations and one-off patches (`patch_16_aums.py`, `update_100_dekkingsgraad.py`). This is where schema evolves.
4. **`scripts/analysis/`** — read-only aggregations producing markdown / plots (`analyze_equity_strategy.py`, `analyze_geo_weights.py`, `analyze_uitvoerder_fiduciair.py`, etc.). `scripts/run_all.sh` chains a subset of these. R script `analyze_strategies.R` exists but is not part of the main flow.
5. **`scripts/utils_and_viz/`** — dashboard, Excel export, null/duplicate audits, dedupers (`dedupe.py`, `dedupe_ing.py`).

## Raw data layout

`.gitignore` excludes all of `data/annual_reports/`, `data/historical_reports/`, `data/interim/`, `data/raw/`, `data/PensioenPro/`, `data/transitieplannen/` — only `data/processed/pension_funds.db` and `data/processed/pension_funds.xlsx` are committed.

PDFs in `data/reports/` and `data/annual_reports/` are typically named `<fund_id>_<FundName>.pdf` (e.g. `106_Hoogovens.pdf`, `73_Ahold_Delhaize.pdf`); the leading integer matches `funds.id` and parsers rely on that mapping.

## Waar de app draait (bijgewerkt 2026-07-30)

Er draaien **twee** frontends op dezelfde database, en dat is de val waar je
anders in trapt:

| | |
|---|---|
| `https://pensioenfondsen.webkowuite.nl` | **R/Shiny-app** op de Mac mini, poort 3852 |
| `http://100.107.33.80:8502` (tailnet) | het Streamlit-dashboard uit deze repo |

De publieke site is dus **niet** het dashboard in `scripts/utils_and_viz/`. Wie
`pension_funds.db` bijwerkt en daarna op het domein kijkt, ziet niets veranderen
tot de Shiny-keten is doorlopen.

**De keten naar de publieke site**, drie schakels die elk stil kunnen falen:

```
git push origin main
  → mini: ~/pensioenfondsen-app          git pull (launchd, dagelijks 06:30)
  → mini: refresh_db.sh                  kopieert de DB naar de Shiny-app (elke 6 uur)
  → Shiny-app                            herleest data/ elke 30 minuten
```

Tussentijds bijwerken met één commando:

```bash
ssh webkowuite@100.107.33.80 'cd ~/pensioenfondsen-app && git pull -q && \
  "$HOME/Library/Mobile Documents/com~apple~CloudDocs/R/pensioen_dashboard/refresh_db.sh"'
```

De R-app zelf staat in `~/Library/Mobile Documents/.../R/pensioen_dashboard/`
(iCloud, buiten deze repo) en heeft een eigen `DEPLOY.md`. Let op: `refresh_db.sh`
wees oorspronkelijk naar een iCloud-clone die op de mini nooit werd
gematerialiseerd — het script eindigde maandenlang op "bron niet gevonden"
zonder dat iemand het merkte. Het wijst nu naar `~/pensioenfondsen-app`.

**De scrape** draait op de mini via launchd `nl.wuite.pensioenfondsen.scrape`
(dagelijks 06:00) → `scripts/automation/scrape_mini.sh` → `scrape_push.sh`:
monitor + nieuws-parser + datakwaliteitscontrole + commit + push. Bij een nieuwe
commit herstart het dashboard en draaien de alerts. De oude MBP-keten met
launchd, een AppleScript-.app en Full Disk Access is sinds 10 juni 2026 uit;
`~/Library/LaunchAgents/nl.wuite.pensioenfondsen.scrape.plist.disabled-20260610`
is wat daarvan over is.

Volledige beschrijving met poorten, tunnel-details en healthchecks:
`scripts/automation/DEPLOY_STATUS.md`. Dat bestand is leidend bij twijfel.

## Controles op de datakwaliteit

Vier scripts, waarvan er één automatisch meedraait:

| script | wanneer | wat |
|---|---|---|
| `db_management/check_data_quality.py` | **elke scrape** | veertien DB-controles, o.a. analyses bij een niet-bestaand of duplicaat fonds |
| `utils_and_viz/audit_deep_dive.py` | met de hand | de cijfers achter de diepteanalyse: optellingen, bereiken, DNB-vergelijking |
| `utils_and_viz/audit_analysis_bronnen.py` | met de hand | hoort de bron-PDF bij het fonds? Vond twaalf analyses over een ander bedrijf |
| `utils_and_viz/audit_pdfs.py` | met de hand | is elk bestand een echte PDF? `--quarantine` verplaatst naar `data/_broken/` |

De laatste drie lezen de PDF's en horen dus **op deze Mac** thuis: de mini heeft
alleen `data/reports/` (die zit in git), niet `data/annual_reports/`.

Twee valkuilen die dat opleverde en die zich zullen herhalen:

- **Een bestandsnaam met het juiste fonds-id zegt niets over de inhoud.** Er
  stonden analyses in de tabel over het Nederlands Filmfonds, FrieslandCampina
  en een tbs-kliniek. Draai `audit_analysis_bronnen.py` na elke download.
- **Vergelijk nooit twee velden zonder hun definitie na te lopen.** DNB's
  `zakelijke_waarden_pct` is breder dan aandelen + vastgoed + alternatives, en
  `equity_allocation_pct` is weer iets anders. Beide keren leverde dat tientallen
  valse meldingen op voordat het kwartje viel.

## Quirks to keep in mind

- All domain terms are Dutch — `dekkingsgraad` (funding ratio), `beleidsdekkingsgraad` (policy funding ratio), `deelnemers` (participants: actief / slapers / gepensioneerd), `toeslag` (indexation), `uitvoerder` (admin provider), `fiduciair_beheerder` (fiduciary manager), `transitieplan`/`invaren`/`Wtp` (the 2024-2027 pension-system transition). Preserve Dutch column names when adding fields.
- Fund websites change often; expect Playwright selectors to rot. Many `scrape_*.py` files have a `_robust` sibling that's the current working version.
- There is no test suite. Files prefixed `test_` (e.g. `test_playwright.py`, `test_dashboard.py`) are scratch/exploration scripts, not unit tests. Verify changes by running the affected script and inspecting the DB or dashboard.
- `scratch_st.py` and `dev_out.txt` at the repo root are throwaway. Don't treat them as documentation.
