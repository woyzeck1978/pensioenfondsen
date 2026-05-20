# ROADMAP / Project State — Dutch Pension Funds Dashboard

Handoff document for the next agent (Antigravity, Claude Code, or a human).
Last updated: 2026-05-20.

This is a sibling to `CLAUDE.md`. CLAUDE.md tells an agent **how to work on
this codebase**. This file tells an agent **what's been done and what's
open**, with concrete first steps for each open item.

---

## What this project is, in one paragraph

A research dataset of 179 Dutch pension funds. A pipeline of scripts in
`scripts/` populates a single SQLite database `data/processed/pension_funds.db`
(committed to git, ~25 MB). A Streamlit app `scripts/utils_and_viz/dashboard.py`
visualises the data; it's deployed to **https://pensioenfondsen.streamlit.app**,
which auto-rebuilds from every push to `main` on
`github.com/woyzeck1978/pensioenfondsen`. A launchd job re-scrapes fund
websites every 2 days and auto-commits the updated DB. A local Ollama
instance on a Mac mini / MacBook Pro over Tailscale is used for LLM
extraction from annual-report PDFs.

---

## Architecture map

```
.streamlit/config.toml         CSU-inspired theme (light, #6554A3 accent)

scripts/
├── automation/
│   └── scrape_push.sh          launchd worker: monitor + parser + commit + push
├── data_collection/
│   ├── fetch_dnb_quarterly.py  Pull DNB statpub JSON via api.dnb.nl
│   ├── monitor_websites_concurrent.py  Crawl ~150 fund sites, populate scraped_documents
│   ├── parse_news_articles.py  Fetch + parse news URLs into news_articles (real title + date)
│   └── scrape_fund_metadata.py Homepage scrape for deelnemers + uitvoerder
├── db_management/
│   ├── backfill_funds_from_dnb.py        Fill funds.aum_euro_bn + beleidsdg from DNB
│   ├── backfill_historical_from_dnb.py   Fill historical_metrics from DNB Q4
│   ├── backfill_history_extras.py        CPI + deelnemers snapshot + indexatie pivot
│   ├── load_dnb_quarterly.py             Pivot DNB JSON into dnb_quarterly_metrics
│   └── normalize_wtp_fields.py           Clean wtp_invaren + wtp_transitie_datum
└── document_parsing/
    ├── extract_pdf_metadata.py             Regex extraction from PDFs
    ├── llm_extract.py                      Ollama: snapshot deelnemers + SFDR + EU taxonomy
    └── llm_extract_deelnemers_history.py   Ollama: multi-year deelnemer tables (NEW, not yet run at scale)

scripts/utils_and_viz/
├── dashboard.py                Main Streamlit app
└── style.css                   CSU-inspired styling

data/processed/pension_funds.db   THE database — committed to git
data/processed/dnb_per_fund_quarterly_raw.json   DNB raw download (5.8 MB, committed)
data/annual_reports/              PDFs — gitignored
data/reports/                     Older PDFs from earlier runs — partly committed
logs/cron/                        launchd run logs — gitignored
logs/llm_extract/                 LLM run logs — gitignored
```

### DB tables (highlights)

| Table | Purpose | Rows |
|---|---|---|
| `funds` | One row per pension fund, ~60 wide columns | 179 |
| `historical_metrics` | Per-year metrics (multi-year overview) | ~2,170 |
| `dnb_quarterly_metrics` | Per-fund × quarter × 14 metrics (2015 Q1–2025 Q4) | 56,747 |
| `monthly_funding_ratios` | Beleidsdekkingsgraad per fund × month | 6,009 |
| `fy_annual_metrics` | Free-form parsed FY values per fund × year | small, for new findings |
| `scraped_documents` | Discovered URLs (PDFs + news) | 6,500+ |
| `news_articles` | Parsed news with real title + ISO date | 2,374 |
| `llm_extracted` | LLM output (snapshot per fund) | 94 |
| `llm_deelnemers_history` | LLM output (per-year deelnemer dict) | empty — script ships unused |
| `extracted_metadata` | Website-scraped metadata candidates | review-only |
| `pdf_extracted` | PDF regex candidates | review-only |

---

## Status: what works

### Data pipeline
- **DNB integration**: 153 of 186 DNB rapporteurs mapped to fund IDs. 56,747 quarterly rows imported. Subscription key (`e0249d4903b049e6844a8bc0c5961ddf`) is public — embedded in the DNB dashboard's JS.
- **News pipeline**: fund websites monitored → URLs in scraped_documents → article text + date in news_articles. Parser hit rate 98% titles / 81% dates.
- **WTP normalization**: invaren values reduced to `{ja, nee, uitgesteld, NULL}`; dates to ISO `YYYY-MM-DD`.

### Automation
- **launchd job** `nl.wuite.pensioenfondsen.scrape` runs every 2 days (172800 s).
- Triggers `~/Applications/PensioenfondsenScraper.app` (AppleScript bundle with Full Disk Access — needed because launchd's TCC context can't read Google Drive's CloudStorage mount).
- Logs go to `logs/cron/`. See CLAUDE.md "Automated bi-daily scrape" section for management commands.

### Deploy
- `main` push → Streamlit Cloud rebuild → live at https://pensioenfondsen.streamlit.app
- `requirements.txt` pinned `streamlit>=1.40` (selection_mode=single-row needs ≥1.35).
- `runtime.txt` pinned `python-3.12` (Cloud doesn't yet support 3.14 which the local machine uses).

### Dashboard pages
- Sector Overview — KPI tiles, scatter, pie, click-to-detail row selector.
- Fund Deep-Dive — per-fund KPIs, FY annual report card, Meerjarenoverzicht (now with 11 years of data for ~145 funds).
- Equity Strategy Deep-Dive — €1–5 Bn cohort.
- Asset Managers Exposure — top-20 by mandate count.
- WTP Tracker — KPIs + planned transitions chart + recent invaren news section.
- Dekkingsgraad Analysis — top/bottom + histogram.
- ESG & SFDR Tracker — Article 6/8/9 distribution.
- Industry News Feed — filterable, sorted by real publication date.
- Begrippenlijst (Glossary).

### NULL coverage on the funds table (snapshot 2026-05-20)

| Field | Filled | NULL | Coverage |
|---|---:|---:|---:|
| AUM | 177 | 2 | 99% |
| Beleidsdekkingsgraad | 161 | 18 | 90% |
| Deelnemers totaal | 130 | 49 | 73% |
| Deelnemers actief | 124 | 55 | 69% |
| Uitvoerder | 148 | 31 | 83% |
| SFDR Article | 104 | 75 | 58% |
| EU Taxonomy | 47 | 132 | 26% |

### NULL coverage on historical_metrics (2,170 rows across 2015-2025)

| Column | Filled |
|---|---:|
| aum_euro_bn | 2,106 |
| beleidsdekkingsgraad_pct | 2,133 |
| vereiste_dekkingsgraad_pct | 1,513 |
| beleggingsrendement_pct | 2,083 |
| zakelijke_waarden_pct | 1,860 |
| rente_afdekking_pct | 1,856 |
| rente_afdekking_rendement_pct | 1,681 |
| cpi_pct | 2,132 (every year × fund row) |
| indexatieverlening_pct | ~60 |
| deelnemers_* | ~120 each (last-year snapshot only) |

---

## Open items, ranked by ROI

### 1. Per-year deelnemers via LLM (HIGH value, MEDIUM effort)

Script ships ready: `scripts/document_parsing/llm_extract_deelnemers_history.py`.
Not yet run at scale because the MBP keeps falling asleep during bulk runs
and the local Ollama becomes unreachable on Tailscale.

**First steps:**
1. On the MBP: System Settings → Battery → "Prevent automatic sleeping when display is off" = ON (Power Adapter).
   Or run `caffeinate -di &` in a terminal there before starting the bulk.
2. Verify Ollama is responding: `curl http://100.71.107.24:11434/api/tags`.
3. Smoke test on 3 known PDFs:
   ```
   cd scripts/document_parsing && python3 llm_extract_deelnemers_history.py --funds 111,24,112
   ```
4. If 2-of-3 succeed: bulk on top-50 by AUM:
   ```
   python3 llm_extract_deelnemers_history.py --top 50 --resume
   ```
5. Apply with NULL-only guard:
   ```
   python3 llm_extract_deelnemers_history.py --apply
   ```
6. Commit + push the DB.

Expected gain: 3-5 years of per-fund deelnemer history for ~30 funds = several hundred extra cells in historical_metrics that currently only have a single-year snapshot.

**Known issue with current filter**: the kerncijfers page-scorer requires both a header keyword AND a thousand-separator number pattern. Small funds (Lloyd's: 351 actief) don't match the numeric regex. Loosen `RE_DEELN_NUMERIC` in the script to accept 3-7 digit numbers without thousand separators if you want to catch them.

### 2. Duplicate-row clean-up in historical_metrics (LOW value, LOW effort)

Some funds have multiple rows for the same (fund_id, year). Hoogovens had 4 rows per year. Dashboard de-dupes for display via `groupby('year').last()`, so this is cosmetic — but a cleaner DB is nicer.

```sql
DELETE FROM historical_metrics
WHERE id NOT IN (
  SELECT MIN(id) FROM historical_metrics GROUP BY fund_id, year
);
```

Run with care: confirm no important non-NULL value exists in the non-MIN-id rows that doesn't exist in the kept row. Probably safe — historical_metrics has been filled mostly by row-level UPDATEs that touched all duplicates.

### 3. KPN funds.aum_euro_bn anomaly (LOW value, LOW effort)

`funds.aum_euro_bn = 1.1` for KPN, but DNB and the FY2025 jaarverslag both
say €10.0 Bn. The 1.1 is plausibly the DC-lifecycle component only. The
Fund Deep-Dive page already shows a warning ("⚠ funds-table values
differ from the annual report") so users see both numbers — but the
canonical funds-table value should probably be 10.0.

Decision needed: is `funds.aum_euro_bn` supposed to be the total fund AUM
(DB + DC + lifecycles), or only the DB-regeling component? Once decided,
overwrite or split into separate columns.

### 4. ABN actief deelnemers = 44 (LOW value, LOW effort)

ABN's `funds.deelnemers_actief = 44` is clearly a stub/typo. Slapers (51,894),
gepens (31,258), and totaal (83,196) look correct. Either NULL the 44 so
LLM can refill it next round, or look it up manually (likely around
1,500-3,000).

### 5. APF-kring → umbrella aggregation (MEDIUM value, MEDIUM effort)

DNB reports HNPF (fund_id 64), DeNAPF (145), Centraal Beheer APF (65),
Stap APF (67), and Unilever APF (68) per-kring (e.g. "Kring Cargill (Hnp)").
Aggregating kring-level data back to the APF umbrella row would fill the
remaining AUM/beleidsdg NULLs for those 5 funds.

Approach: identify each umbrella's kringen via name pattern, sum kring
AUM, weighted-average kring beleidsdg by AUM share, write to the umbrella row.

### 6. SFDR / EU taxonomy gaps (MEDIUM value, HIGH effort)

After all our extraction passes:
- SFDR NULL: 75 funds (mostly without a local PDF, or with PDFs that don't mention an Article 6/8/9 classification).
- EU Taxonomy NULL: 132 funds (taxonomy reporting is recent and many small funds simply don't report it).

The remaining gaps need either bigger PDFs (download more FY2024 jaarverslagen from scraped_documents URLs) or a manual data entry pass.

### 7. Data-quality outlier detection (MEDIUM value, LOW effort)

The Hoogovens-style "162% rendement" issue was found by `ABS(value) > 50`.
Add similar sanity sweeps periodically:

```sql
-- Outliers across historical_metrics
SELECT 'rendement >50%' AS issue, COUNT(*) FROM historical_metrics WHERE ABS(beleggingsrendement_pct) > 50
UNION SELECT 'beleidsdg outside 50..250', COUNT(*) FROM historical_metrics WHERE beleidsdekkingsgraad_pct NOT BETWEEN 50 AND 250
UNION SELECT 'aum <=0 or >1000 Bn', COUNT(*) FROM historical_metrics WHERE aum_euro_bn <=0 OR aum_euro_bn > 1000
;
```

### 8. Streamlit-aggrid for true click-to-detail table (LOW value, MEDIUM effort)

Sector Overview's Fund Directory uses Streamlit's native dataframe with
row selection. A nicer UX would be inline cell badges (category color,
status pill) which native dataframe can't render. `streamlit-aggrid` would
fix this but adds a dependency. Not done because the current UX works fine.

---

## Environment details for the next agent

| Thing | Value |
|---|---|
| GitHub repo | `github.com/woyzeck1978/pensioenfondsen` (public) |
| Default branch | `main` |
| Streamlit Cloud URL | `https://pensioenfondsen.streamlit.app` |
| User identity in commits | `Webko Wuite <webkowuite@mac.home>` (auto-derived; run `git config user.email …` to fix) |
| Local Python | 3.14.2 |
| Cloud Python | 3.12 (pinned in `runtime.txt`) |
| DNB API host | `api.dnb.nl/statpub-intapi-prd/v1/` |
| DNB API key | `e0249d4903b049e6844a8bc0c5961ddf` (public, in dashboard JS) |
| Ollama host | `100.71.107.24:11434` (MBP via Tailscale) — also `mac-mini-van-webko` at `100.107.33.80` but no Ollama there |
| Ollama models | `mistral-small` (preferred for Dutch NL), `llama3.1:8b`, `deepseek-r1:14b` |
| launchd label | `nl.wuite.pensioenfondsen.scrape` |
| .app bundle for FDA | `~/Applications/PensioenfondsenScraper.app` |
| Local wrapper script | `~/bin/pensioenfondsen_scrape.sh` |

## Files that exist outside the repo (machine-specific)

These need to be recreated on a new machine:
- `~/Applications/PensioenfondsenScraper.app` — AppleScript bundle for launchd FDA
- `~/bin/pensioenfondsen_scrape.sh` — local launcher
- `~/Library/LaunchAgents/nl.wuite.pensioenfondsen.scrape.plist` — launchd spec

See CLAUDE.md "Automated bi-daily scrape (launchd + .app bundle)" section for the recreate instructions.

---

## Working style notes for the next agent

- Project language is Dutch for user-facing copy, English for code identifiers and commit messages.
- User responds well to concrete plans with effort/cost estimates before action. Ask before spending money (e.g. Claude API) or doing 5+ hour batch jobs.
- The user prefers NULL-only fill guards over overwrite — never replace a hand-curated value automatically; surface a mismatch warning instead (see Fund Deep-Dive's FY card for the pattern).
- When committing, prefer multi-paragraph messages that explain *why*. The git log is the only persistent record of decisions.
- The `--dry-run` pattern is used a lot — always offer a preview pass before bulk DB writes.
- For PDF extraction: the regex route is fragile because of multi-year tables and table-text merging. LLM is more robust but local LLM stability (MBP sleep) is the main bottleneck.
