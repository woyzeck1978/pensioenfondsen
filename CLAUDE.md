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

## Automated bi-daily scrape (launchd + .app bundle)

A launchd agent re-scrapes all fund websites every 2 days, parses the new
news URLs, and pushes the updated `pension_funds.db` to `origin/main`. The
Streamlit Cloud app rebuilds from main automatically.

The call chain has three hops to work around macOS TCC restrictions:

```
launchd
  → ~/Applications/PensioenfondsenScraper.app/Contents/MacOS/applet
  → /Users/webkowuite/bin/pensioenfondsen_scrape.sh
  → scripts/automation/scrape_push.sh (here, in the repo)
```

Why so many hops:

1. **launchd cannot run shell scripts that live in Google Drive's
   `CloudStorage` mount** — macOS TCC denies "operation not permitted" on
   ordinary `read` of Drive files for launchd-spawned processes.
2. **An AppleScript .app bundle** at `~/Applications/PensioenfondsenScraper.app`
   gets a distinct TCC identifier and can be granted **Full Disk Access**
   via System Settings → Privacy & Security → Full Disk Access. The app's
   `do shell script` invokes the local launcher inheriting that TCC right.
3. **The thin wrapper at `~/bin/pensioenfondsen_scrape.sh`** sits outside
   Drive (so it loads even before FDA propagates) and exec's the real
   worker via `zsh -s < <drive-path>`.

Files:
- `scripts/automation/scrape_push.sh` — the actual worker (in repo). Pre/post
  counts, abort on script failure, skip commit if DB byte-identical, push
  retry 3× with 30s back-off.
- `~/bin/pensioenfondsen_scrape.sh` — local launcher outside Drive (not in
  repo; recreate with the snippet documented in this file).
- `~/Applications/PensioenfondsenScraper.app` — AppleScript bundle (not in
  repo; recreate via `osacompile -e 'do shell script
  "/Users/webkowuite/bin/pensioenfondsen_scrape.sh"'` then add
  `CFBundleIdentifier=nl.wuite.pensioenfondsenscraper` and
  `LSUIElement=true` to its `Contents/Info.plist`, then ad-hoc resign with
  `codesign --force --sign -`).
- `~/Library/LaunchAgents/nl.wuite.pensioenfondsen.scrape.plist` — the
  launchd job spec. `StartInterval=172800`, logs to `logs/cron/`.

One-time setup on a new Mac:
1. Build the .app bundle (see osacompile command above).
2. Open System Settings → Privacy & Security → Full Disk Access.
3. Trigger the prompt by running the .app once: `open -a "PensioenfondsenScraper"`.
4. Click Allow on the macOS prompt — the app now appears in the FDA list
   with a green toggle.
5. Drop the launchd plist into `~/Library/LaunchAgents/` and bootstrap.

Managing:

```bash
# Status (PID if running, last exit code if not)
launchctl list | grep pensioen

# Fire it now — does NOT change the schedule
launchctl kickstart -p gui/$(id -u)/nl.wuite.pensioenfondsen.scrape

# Pause / resume the recurring job
launchctl bootout   gui/$(id -u) ~/Library/LaunchAgents/nl.wuite.pensioenfondsen.scrape.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/nl.wuite.pensioenfondsen.scrape.plist

# Inspect last run
ls -t logs/cron/*.log | head -1 | xargs tail -50
# Or via launchd's combined stream:
tail logs/cron/launchd.out logs/cron/launchd.err
```

Caveats:
- Mac must be awake at the fire time (or wake before the next interval).
  launchd runs a missed job the moment the Mac wakes up.
- Auto-commits land as `Webko Wuite <webkowuite@mac.home>`. Run
  `git config user.email …` once in the repo to fix attribution.
- Two fund sites (`pensioencg.nl`, `pnb.nl`) sit behind Cloudflare and
  return a "Challenge Validation" page — the script catalogues those
  URLs but can't parse real titles. Acceptable noise for now.
- If `~/Applications/PensioenfondsenScraper.app` is rebuilt or moved,
  the FDA grant may need to be re-confirmed via the same prompt flow.

## Quirks to keep in mind

- All domain terms are Dutch — `dekkingsgraad` (funding ratio), `beleidsdekkingsgraad` (policy funding ratio), `deelnemers` (participants: actief / slapers / gepensioneerd), `toeslag` (indexation), `uitvoerder` (admin provider), `fiduciair_beheerder` (fiduciary manager), `transitieplan`/`invaren`/`Wtp` (the 2024-2027 pension-system transition). Preserve Dutch column names when adding fields.
- Fund websites change often; expect Playwright selectors to rot. Many `scrape_*.py` files have a `_robust` sibling that's the current working version.
- There is no test suite. Files prefixed `test_` (e.g. `test_playwright.py`, `test_dashboard.py`) are scratch/exploration scripts, not unit tests. Verify changes by running the affected script and inspecting the DB or dashboard.
- `scratch_st.py` and `dev_out.txt` at the repo root are throwaway. Don't treat them as documentation.
