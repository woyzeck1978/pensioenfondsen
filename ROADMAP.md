# ROADMAP / Project State — Dutch Pension Funds Dashboard

Handoff document for the next agent (Antigravity, Claude Code, or a human).
Last updated: 2026-06-09.

This is a sibling to `CLAUDE.md`. CLAUDE.md tells an agent **how to work on
this codebase**. This file tells an agent **what's been done and what's
open**, with concrete first steps for each open item.

> **Bijgewerkt 2026-07-31.** Van de tien openstaande punten zijn er acht
> afgehandeld; punt 5 is verworpen met reden. Wat resteert is het
> aggrid-tabelcomponent (punt 8, cosmetisch) en de acht kringen van De
> Nationale, die achter een WAF zitten die elke browserpoging met 403 afwijst.
> De datakwaliteitscontrole ging van 61 bevindingen naar 3, en die drie zijn
> geen fout maar terechte signaleringen.

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

### Dekking van de fondsentabel (stand 2026-07-31, 181 pensioenfondsen)

| Veld | Gevuld | Leeg | Dekking |
|---|---:|---:|---:|
| Uitvoerder | 178 | 3 | 98% |
| Beleidsdekkingsgraad | 173 | 8 | 96% |
| AUM | 168 | 13 | 93% |
| Dekkingsgraad | 136 | 45 | 75% |
| Deelnemers totaal | 119 | 62 | 66% |
| SFDR-artikel | 92 | 89 | 51% |
| Deelnemers actief | 81 | 100 | 45% |
| EU-taxonomie | 44 | 137 | 24% |
| Invaardatum | 32 | 149 | 18% |

**Let op: sommige percentages zijn gedáald sinds mei, en dat is winst.**
Deelnemers actief stond op 124 gevuld en staat nu op 81. Er zijn geen gegevens
verdwenen; er zijn verkeerde gegevens weggehaald. Philips stond op 30 deelnemers
bij 17,65 miljard, HAL op 2,65 miljoen bij 166 miljoen, en zeven fondsen hadden
twee identieke deelnemerskolommen doordat een parser een kolom was opgeschoven.
Wie dekkingspercentages als voortgangsmaat gebruikt, wordt beloond voor het laten
staan van onzin. De datakwaliteitscontrole is de betere maat: die ging van 61
bevindingen naar 3, en die drie zijn geen fout.

Het aantal fondsen daalde van 190 naar 181 doordat verzekeraars, PPI's,
duplicaten en drie Nederlandse regelingen bij een Belgische OFP als
`is_pensioenfonds = 0` zijn gemarkeerd, met de reden in `afbakening_reden`. Het
sectortotaal staat daarmee op 1.617,1 miljard.

### Dekking van historical_metrics (1.524 rijen, 2015-2025)

| Kolom | Gevuld |
|---|---:|
| beleidsdekkingsgraad_pct | 1.500 |
| aum_euro_bn | 1.496 |
| beleggingsrendement_pct | 1.476 |
| deelnemers_totaal | 313 |
| deelnemers_actief | 288 |
| indexatieverlening_pct | 187 |
| nominale_dekkingsgraad_pct | 34 |
| solidariteitsreserve_pct | 1 |

De deelnemerskolommen zijn ruim verdubbeld doordat een kerncijfertabel vijf
jaargangen naast elkaar toont: één verslag over 2025 vult ook 2021 tot 2024. Zie
`scripts/db_management/lees_deelnemers_tabel.py --alle-jaren`.

### Analyses

204 analyses over 157 fondsen, waarvan 101 over boekjaar 2025. Daarnaast 65
cohortwaarnemingen in `cohort_metrics` — rendement per leeftijdsgroep en
risicoprofiel, de maatstaf die onder de Wtp in de plaats komt van een
dekkingsgraad voor het hele fonds.

---

## Sessie 2026-05-20..22 — features A–F shipped

Six-category enhancement pass; all but a handful of skipped items done.

**A. Vergelijken & exporteren**
- ✅ **A1** Fund Comparison page (new in sidebar) — multi-select up to 3
  funds, side-by-side KPI cards + 4 overlay charts (beleidsdg / AUM /
  rendement / deelnemers).
- ✅ **A2** Peer-group overlay on Fund Deep-Dive's Historical Performance —
  toggle adds dotted category-average lines for beleidsdg and rendement.
- ✅ **A3** PDF Factsheet download per fund (matplotlib + PdfPages, no
  new deps). 1-page A4 with header, 5 KPI tiles, beleidsdg chart,
  AUM bars, deelnemers stacked area, footer.
- ✅ **A4** Excel export of Fund Directory on Sector Overview (xlsxwriter,
  formatted columns + frozen header).

**B. UX polish**
- ✅ **B1** Typeahead — already built into st.selectbox; no change needed.
- ✅ **B2** Recent-viewed funds — sidebar buttons (max 5), session-state-backed.
- ✅ **B3** Mobile responsive CSS — @media (max-width: 768px) and 480px;
  KPI tiles drop to 2-col then 1-col, headings shrink.
- ✅ **B4** Print CSS — @media print hides chrome, white bg, break-inside:avoid.

**C. New analytics pages**
- ✅ **C1** Trends page — biggest movers in beleidsdg + AUM over 1/4/8/12 quarters.
- ✅ **C2** Invaren timeline on WTP Tracker — scatter chart, status-colored,
  AUM-sized, with today reference line.
- ✅ **C3** Indexatie vs CPI on Fund Deep-Dive — bars + line + cumulative
  koopkracht caption.
- ✅ **C4** Beleggingsprofiel/rente-afdekking line chart on Fund Deep-Dive
  using the new zakelijke_waarden_pct + rente_afdekking_pct columns.

**D. Engagement / live data**
- ✅ **D1** Watchlist — pin/unpin button on Deep-Dive, '⭐ Watchlist'
  quick-view block on Sector Overview. Session-scoped state.
- ✅ **D2** Recent WTP news digest on Sector Overview landing.
- ⏭ **D3** Email/Slack alerts — skipped intentionally; needs a separate
  server-side service (FastAPI or GitHub Actions) outside Streamlit Cloud.
- ✅ **D4** RSS export of Industry News Feed filter — RSS 2.0 XML download.

**E. Data quality**
- ✅ **E1** Smart dedupe of historical_metrics — 2159 rows → 1543. For each
  duplicate (fund_id, year) pair, kept the row with the most non-NULL cells.
- ✅ **E2** KPN AUM 1.1 → 10.0 (the FY2025 jaarverslag's DB-regeling number).
  The Deep-Dive page's mismatch warning disappears.
- ✅ **E3** ABN AMRO actief 44 → NULL (was a stub; let a future LLM pass refill).
- ⏭ **E4** Retry the 66 LLM-error funds — needs MBP to stay awake. Documented.

**F. Infrastructure**
- ✅ **F1** Database indexes — added idx_hist_fund_year, idx_hist_year,
  idx_dnbq_fund_year, idx_dnbq_metric, idx_news_pub, idx_news_fund,
  idx_scrapdoc_fund_type. ANALYZE run.
- ⏭ **F2** REST/GraphQL API endpoint — out of scope without a concrete
  consumer. Documented as future option requiring a separate FastAPI service.
- ✅ **F3** ROADMAP updated (this section).

## Sessie 2026-05-22..23 — jaarverslag-analyse via LLM bootstrapped

- ✅ Tabel `fund_analysis` (commit `d682258`): summary + highlights /
  lowlights / risks (JSON-arrays) + transitie_status per fund × FY.
- ✅ `scripts/document_parsing/llm_extract_analysis.py` driver: scores
  pagina's op header-keywords + zinsdichtheid, sendt top-5 als JSON-
  mode prompt naar Ollama `mistral-small` (Tailscale).
- ✅ Page-selector v2 (commit `b011291`): TOC-detector +
  `sentence_score>=2`. Fund 13 ging van "raad-van-toezicht-goedgekeurd"
  boilerplate naar concrete risicotaxonomie + dekkingsgraad-uitspraken.
- ✅ Inventory-filter (this commit): skip `*_Transitieplan.pdf`,
  `*_ESG.pdf`, `*_SFDR.pdf`, infographics — anders kiest de inventory
  alphabetisch het verkeerde document (Hoogovens kreeg het transitieplan
  i.p.v. het jaarverslag).
- ✅ Top-30 run gedraaid: 22 nieuwe rijen in `fund_analysis`. 12 FY2024,
  3 FY2025, 1 FY2023, 8 FY0 (oude PDFs zonder jaartal — zie open item
  #9 voor herstelplan).
- ✅ Dashboard rendert het blok onder Fund Deep-Dive's KPI-card al
  (commit `d682258`).

## Sessie 2026-05-24..25 — news_articles publicatiedatum-cleanup

Het news-feed in de dashboard toonde voor veel rijen de **crawl-datum**
in plaats van de **publicatiedatum** van het artikel. Drie oorzaken
gefixt, in volgorde:

- ✅ **Parser-fallback gestopt** (commit `b71bc42`). `parse_published_date`
  in `scripts/data_collection/parse_news_articles.py` accepteerde
  voorheen vandaag's datum als "weaker signal" wanneer de body-text-
  scan geen oudere datum vond. Resultaat: voor login-walls, JS-gated
  pagina's, landing-pages en profiel-pickers werd de crawl-datum
  opgeslagen als publicatiedatum. Toegevoegd: `trust_today=True`-param,
  alleen gezet door structured-source callers (`<time>`, `<meta
  article:published_time>`). Body-text caller default `False` —
  vandaag-only in body is bijna altijd een "laatst bijgewerkt" footer.
- ✅ **URL-slug backfill** (commit `ff75562`). 673 rijen achteraf gefixt
  door de publicatiedatum uit de URL-slug te halen waar die er als
  YYYYMMDD, ISO `/YYYY/MM/DD/` of Dutch `/DD-MM-YYYY/` instaat. De
  Dutch-DMY pattern is nieuw — toegevoegd aan zowel parser als
  backfill-script omdat bpfschilders een URL had met zowel
  `/nieuws-2026/` als `/11-05-2026-…` en de ISO-matcher die laatste
  als 2026-11-05 interpreteerde i.p.v. 11 mei.
- ✅ **HTTP-fetch tier** (commit `c429aa1`). `fix_news_dates.py`
  herschreven: concurrent (12 workers, 10s timeout), alleen op
  NULL-rijen, skipt garbage-titels up-front, valideert dat een
  gefetcht datum niet ná de scrape-datum ligt. 317 URLs gefetcht,
  66 nieuwe datums uit `<meta>`/`<time>`/JSON-LD.

**Eindstand `news_articles`:**

| State | Vóór | Na |
|---|---|---|
| Met echte publicatiedatum | 1932 (deels nep) | 1974 |
| NULL (verborgen in dashboard) | 475 | 433 |
| Future-date (logisch onmogelijk) | 3 | 0 |
| Garbage-titel met datum | 114 | 0 |
| Fallback-misfires (= scrape-date) | 178 | 0 |

**Onfixbaar zonder zwaardere middelen** (verklaart de 433 NULL):
- ~179 Cloudflare "Challenge Validation" (`pensioencg.nl`, `pnb.nl`)
  — site geeft geen content terug, scraper ziet alleen het JS-puzzle
- ~50 garbage landing-pages (`Nieuws`, `Sign in`, etc.)
- ~200 echte artikelen zonder `<meta>`/`<time>`/JSON-LD-datum én zonder
  datum in URL-slug — LLM-extractie op pagina-tekst zou kunnen werken
  maar staat niet op de roadmap.

## Sessie 2026-05-25 — ABP FY2025 + dashboard-fix

Per-fund workflow voor het ophalen van een net-verschenen FY2025
jaarverslag uitgewerkt, met ABP als template. Plus een latente
dashboard-bug ontdekt en gefixt.

- ✅ **News-radar query** — om te detecteren welke fondsen recent een
  FY2025-jaarverslag hebben aangekondigd:
  ```sql
  SELECT f.id, f.name, n.published_date, n.title
  FROM news_articles n JOIN funds f ON n.fund_id=f.id
  WHERE n.title LIKE '%jaarverslag%2025%'
    AND n.published_date IS NOT NULL
    AND date(n.published_date) >= '2026-01-01';
  ```
  Bevestigd op 2026-05-25: ABP (24 april), Beroepsvervoer (28 april),
  KPN (13 mei) hebben FY2025 uitgebracht. Werkt alleen sinds de
  news-publicatiedatums in deze week zijn schoongemaakt (zie vorige
  sessie).
- ✅ **ABP FY2025 verwerkt** (commit `7cb2267`). PDF gedownload van
  `https://jaarverslag.abp.nl/abp-jaarverslag-2025.pdf` (6.6 MB, 221p),
  opgeslagen als `data/annual_reports/9_ABP_2025.pdf` (gitignored).
  FY0-rij verwijderd, `llm_extract_analysis.py --funds 9` produceerde
  nieuwe FY2025-samenvatting met concrete content (beleidsdekkingsgraad
  118,3%, invaarbesluit ontvangen). Tijd: ~2.5 min op MBP via
  Tailscale.
- ✅ **Dashboard-bug gefixt** (commit `391a384`). Het Analyse-jaarverslag
  expander-block in `scripts/utils_and_viz/dashboard.py` zat genest in
  `if not fy_df.empty:` — gekoppeld aan presence van
  `fy_annual_metrics`-rijen. ABP heeft geen rijen in die tabel
  (alleen `fund_analysis`), dus de freshly-extracted samenvatting was
  onzichtbaar in de UI. Block losgekoppeld; titel haalt fiscal_year nu
  uit `fund_analysis` zelf.
- ✅ **Visueel geverifieerd** via Playwright voor alle 4 FY2025-funds
  (ABP, Beroepsvervoer, PGB, KPN). Alle 4 expanders renderen.
- ✅ **BPL + Achmea FY2024 verwerkt** (commit `3f09895`). Via een
  bredere lookup op `scraped_documents` (waar de scraper al PDF-URLs
  had ontdekt) gevonden:
  - 16 BPL: `https://www.bplpensioen.nl/sites/default/files/documenten/bpl-pensioen-jaarverslag-2024.pdf`
  - 72 Achmea: `https://www.pensioenfondsachmea.nl/-/media/Files/Achmea/Pensioen-123-laag-3-algemeen/Pensioenfonds-Achmea-Jaarverslag-2024.pdf`
    (Achmea: `--http1.1` flag bij curl nodig, hun server gaf HTTP/2
    stream-error onder default config — exit 92.)
- ✅ **PFZW scraper-fix** (commit `fa0fd7e`). Drie aaneenschakelende
  problemen die PFZW's nieuwere PDFs onzichtbaar maakten:
  - `funds.website` stond op `pfzw.nl/en/about-us.html` — Engelse
    subboom, geen link naar Nederlandse jaarverslag-index. Geüpdatet
    naar `pfzw.nl/`.
  - URL-pattern was veranderd: oude PDFs onder
    `/content/dam/pfzw/over-ons/jaarverslag/pdf/`, nieuwe (FY2024+)
    onder `/content/dam/pfzw/web/over-ons/jaarverslagen/`. Direct-
    URL-guesses op het oude pattern returnden allemaal 404.
  - Scraper-`paths_to_check` lijst was statisch (`/`, `/nieuws`,
    `/actueel`, `/documenten`, `/downloads`, `/over-ons/nieuws`,
    `/over-het-fonds/documenten`) en bereikte
    `/over-pfzw/dit-presteren-we/jaarverslagen.html` nooit.

  Fixes in `scripts/data_collection/monitor_websites_concurrent.py`:
  - **One-hop deep-crawl**: na de 7 vaste paths scant de scraper de
    homepage-links op `/jaarverslag|annual.?report|publicatie/i` en
    volgt die één hop dieper (cap 5 per fund). Levert nu alle 12
    PFZW-jaarverslagen FY2015–FY2025 op in `scraped_documents`
    (was 5).
  - **`--funds <ids>` flag**: targeted re-scrape zonder op de bi-daily
    launchd te wachten.
- ✅ **5 andere funds met `/en/` URL** geüpdatet naar Dutch homepage
  (32 PGB, 36 StiPP, 43 BPZ, 45 IBM, 142 Zwitserleven PPI). Volgende
  bi-daily picks-up nieuwe FY2025-PDFs bij deze 5 automatisch.
  Twee funds blijven uit scope: 69 ASW (DNB-register als URL),
  144 ASR (one-off persrelease als URL) — beide al uitgesloten van
  Fund Deep-Dive in `dashboard.py:685`.
- ✅ **PFZW FY2025 verwerkt** (commit `88e4f6a`). 33 MB / 219 pages.
  Output: invaardatum 1 januari 2026, geen AVG-incidenten,
  waarderings-risico op subjectieve inputs.

**FY2025-stand einde sessie:** 5 fondsen verwerkt (9 ABP, 13 Beroeps-
vervoer, 32 PGB, 41 PFZW, 111 KPN). Volgende publicatie-golf in
juni/juli; bi-daily scraper zal nieuwe PDFs nu automatisch oppikken
voor de 6 fondsen met gefixte website-URLs.

## Sessie 2026-06-09 — self-host enhancements Fase 0–4 (live op main)

Aanleiding: het dashboard draait nu **self-hosted** op de Mac mini achter
Cloudflare Tunnel (`pensioenfondsen.webkowuite.nl`). Dat heft drie
Streamlit-Cloud-plafonds op: geen persistente schijf, geen bereikbare
Ollama/secrets, en RAM/sleep-limieten. Daardoor werden eerder geskipte
items (D3 alerts, F2 API) plots haalbaar. **Alle vijf fasen staan gemerged
op `main` en draaien live** (eind-commit `eb4dd83`); de feature-branch is
opgeruimd.

**Deploy-realiteit (belangrijk voor de volgende agent).** De live app draait
NIET uit deze Drive-repo maar uit een aparte clone **`~/pensioenfondsen-app`
op de mini onder Python 3.9.6**. Nieuwe code moet 3.9-proof zijn:
`from __future__ import annotations` bovenaan elk bestand met `X | None`-
annotaties (FastAPI-routes hebben bovendien `Optional[...]` nodig i.p.v.
`X | None`, want die hints worden runtime ge-evalueerd). De pull-job
(`~/bin/pensioenfondsen_pull.sh`, dagelijks 06:30) doet `git pull --ff-only`
op `main` en herstart het dashboard bij nieuwe commits.

**Architectuurbeslissing — gescheiden writable DB.** Het dashboard schrijft
NOOIT in `pension_funds.db` (die de bi-daily scraper schrijft én pusht).
Alle dashboard-state gaat naar een aparte, gitignored
**`data/processed/app_state.db`** (WAL-mode), zodat handmatige edits elke
scraper-run + `git pull` overleven zonder merge-conflict. Nieuwe module:
`scripts/utils_and_viz/local_state.py`.

- ✅ **Fase 0 — overrides-fundering** (commit `7caba2a`). `local_state.py`
  met tabellen `overrides`, `edit_audit`, `watchlist`, `alerts`. Correcties
  op `funds.<kolom>` worden bij het lezen over de bron-waarde gelegd via
  `apply_overrides(df)` (aangeroepen direct na `get_all_funds()`). Promotie
  naar de bron-DB is een aparte, expliciete stap (`promote_override`).
- ✅ **Fase 1 — Datacuratie-pagina** (commit `7b7ea57`). Nieuwe dashboard-
  pagina (alleen zichtbaar als de write-laag laadt). Veld bewerken met
  zichtbare bron-waarde, **NULL-only-guard standaard aan** (gevulde waarde
  overschrijven = guard bewust uit), Wis/Promoot, en wijzigingslog uit
  `edit_audit`. Vervangt wegwerp-scripts voor ROADMAP #3/#4. Visueel
  geverifieerd via Playwright (guard-block + opslaan + feed-update).
- ✅ **Fase 2 — watchlist + alerts** (commit `629aa88`). Realiseert het in
  D3 overgeslagen alert-idee.
  - Watchlist van `session_state` → `app_state.db` (op stabiel `fund_id`);
    overleeft refresh. UI blijft naam-gebaseerd.
  - `scripts/automation/generate_alerts.py`: scant de net-gescrapete DB op
    drie signalen — **nieuws** (gevolgd fonds), **jaarverslag** (FY-radar,
    `--all-funds`), **beleidsdekkingsgraad-drempelkruising** (110/105/100 op
    de laatste twee maandpunten uit `monthly_funding_ratios`). Idempotent
    via `dedup_key` (INSERT OR IGNORE). Levering via pluggable `notify()`,
    standaard **uit** — feed-only; outbound (ntfy/webhook) alleen bij gezette
    env-var. Flags: `--dry-run`, `--days`, `--thresholds`, `--all-funds`.
  - Dashboard: sidebar-blok "🔔 Meldingen (n)" met ongelezen-badge, klikbare
    titels, type-iconen, markeer-als-gelezen.
  - **Alert-engine draait op de MINI, niet op de MBP-scraper** (commit
    `69d2c09`). Reden: de scraper draait op de MBP met een eigen
    `app_state.db`, maar de live app + watchlist + feed leven op de mini —
    een hook in `scrape_push.sh` zou alerts in de verkeerde DB schrijven
    (split-brain). Daarom roept `~/bin/pensioenfondsen_pull.sh` op de mini
    ná de pull `generate_alerts.py --all-funds` aan met de deploy-venv. De
    hook in `scrape_push.sh` is verwijderd.
- ✅ **Fase 3 — live text-to-SQL** (commits `6e86b42`, `41f2197`). Pagina
  "Vraag het de data": NL-vraag → `qwen2.5:7b-instruct` (Ollama draaide
  **al** op de mini, localhost:11434 — geen verhuizing/installatie nodig) →
  SQLite SELECT → alleen-lezen uitgevoerd → tabel + auto-grafiek. Module
  `scripts/utils_and_viz/text2sql.py` met twee veiligheidslagen: statische
  guard (één SELECT/WITH, geen DML/DDL, geen chaining) + uitvoering op een
  `mode=ro`-connectie. Endpoint/model via env (`PENSIOEN_OLLAMA_URL/MODEL`).
  Resultaat in `session_state` (rendering-only-op-klik bleek fragiel).
- ✅ **Fase 4 — read-only REST API-sidecar** (commit `eb4dd83`). FastAPI
  `scripts/api/main.py` op **0.0.0.0:8503**, **Tailscale-only**
  (100.107.33.80:8503), NIET via de tunnel, geen auth. Endpoints
  `/api/health|funds|funds/{id}|historical/{id}|news`, docs op `/api/docs`.
  Read-only (`mode=ro`) + dashboard-overrides toegepast. launchd-job
  `nl.wuite.pensioenfondsen.api` (plist-template in `scripts/automation/`),
  deps in `scripts/api/requirements.txt` (3.9-venv). Leest live uit de DB →
  hoeft niet herstart bij data-updates.

**Operationele fixes deze sessie:**
- **py39-deploy-blokkade** opgelost: `dashboard.py` had `str | None` zonder
  future-import → crashte op de 3.9-mini. Future-import toegevoegd; de losse
  `fix/dashboard-py39-annotations`-branch (die dit los fixte) is opgegaan in
  `main` en verwijderd. Deploy-clone trackt nu `main`.
- **Cloudflare-tunnel `http2` → `quic`**: de connector draaide met
  `--protocol http2`, waardoor langlopende Streamlit-WebSocket-runs (knop-
  submits, form-saves) hun resultaat NIET terugleverden over de publieke
  URL — pagina rendert wel, interactie "doet niets". Diagnose: identieke
  klik faalt publiek maar slaagt via SSH-port-forward (tunnel omzeild). Fix:
  `--protocol http2` uit `~/Library/LaunchAgents/com.webkowuite.cloudflared.plist`
  (= default quic) + job herladen; ook een dubbele wees-connector opgeruimd.
  Herstelt app-brede interactiviteit. (Plist op de mini, niet in de repo;
  `.bak` aanwezig.)
- **Scrape-cadans** van 2-daags → **dagelijks**.
- **Scrape VERPLAATST van MBP → mini** (10 juni). Symptoom: nieuws liep 2
  weken achter (laatste run 26 mei). Oorzaak: de MBP-keten (launchd → .app
  met Full-Disk-Access → wrapper → `scrape_push.sh`, nodig wegens TCC op de
  Drive-mount) faalde stil — vermoedelijk FDA vervallen, plus MBP-slaap. De
  mini draait altijd en heeft de repo als gewone clone (geen Drive/TCC), dus
  de hele hack vervalt. Nieuw: `scripts/automation/scrape_mini.sh`
  (orchestrator) + launchd `nl.wuite.pensioenfondsen.scrape` op de mini
  (06:00 dagelijks). `scrape_push.sh` portabel gemaakt
  (`PENSIOEN_PROJECT_DIR`/`PENSIOEN_PYTHON`) + rebase-op-push-afwijzing.
  `parse_news_articles.py` had óók de py39-`X | None`-bug → future-import.
  bs4+lxml in de mini-venv (pipeline gebruikt GEEN Playwright). MBP-job
  uitgezet (`.plist.disabled-20260610`). End-to-end via launchd geverifieerd.

**Te beslissen / open:**
- **`app_state.db` ligt in de Drive-map** op de MBP-scrapekant (gitignored);
  de mini-deploy heeft z'n eigen `app_state.db` in `~/pensioenfondsen-app`
  (lokaal, niet in Drive — goed). Eén host = prima; let op bij multi-host.
- Alert-engine `notify()` is feed-only gekozen; ntfy/webhook staan klaar
  maar zijn niet geactiveerd.
- Energie-dashboard deelt dezelfde cloudflared-connector; profiteert van de
  quic-fix maar is niet apart op z'n publieke URL geverifieerd.

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

### 2. Duplicate-row clean-up in historical_metrics (AFGEROND 2026-07-31)

Geen enkele (fund_id, year) komt nog meer dan eens voor, op 1.524 rijen. Opgelost door eerdere opruimacties; de SQL hieronder is niet meer nodig.

<details><summary>oorspronkelijke tekst</summary>


Some funds have multiple rows for the same (fund_id, year). Hoogovens had 4 rows per year. Dashboard de-dupes for display via `groupby('year').last()`, so this is cosmetic — but a cleaner DB is nicer.

```sql
DELETE FROM historical_metrics
WHERE id NOT IN (
  SELECT MIN(id) FROM historical_metrics GROUP BY fund_id, year
);
```

Run with care: confirm no important non-NULL value exists in the non-MIN-id rows that doesn't exist in the kept row. Probably safe — historical_metrics has been filled mostly by row-level UPDATEs that touched all duplicates.

</details>

### 3. KPN funds.aum_euro_bn anomaly (AFGEROND)

`funds.aum_euro_bn` staat op 10.0 en de jaarreeks geeft 10.0 over 2025 — gelijk aan DNB en het jaarverslag. De 1,1 is verdwenen.

<details><summary>oorspronkelijke tekst</summary>


`funds.aum_euro_bn = 1.1` for KPN, but DNB and the FY2025 jaarverslag both
say €10.0 Bn. The 1.1 is plausibly the DC-lifecycle component only. The
Fund Deep-Dive page already shows a warning ("⚠ funds-table values
differ from the annual report") so users see both numbers — but the
canonical funds-table value should probably be 10.0.

Decision needed: is `funds.aum_euro_bn` supposed to be the total fund AUM
(DB + DC + lifecycles), or only the DB-regeling component? Once decided,
overwrite or split into separate columns.

</details>

### 4. ABN actief deelnemers = 44 (GEDEELTELIJK, 2026-07-31)

De 44 is weg; het veld staat nu leeg. Automatisch bijvullen uit het jaarverslag lukte niet: de tabellezer vindt 6.517 slapers en 10.522 gepensioneerden, terwijl de fondsentabel 51.894 en 31.258 heeft. Dat is een factor acht verschil, dus de lezer heeft een tabel van één regeling binnen het fonds te pakken. Handmatig opzoeken blijft nodig; een leeg veld is beter dan een verkeerd veld.

<details><summary>oorspronkelijke tekst</summary>


ABN's `funds.deelnemers_actief = 44` is clearly a stub/typo. Slapers (51,894),
gepens (31,258), and totaal (83,196) look correct. Either NULL the 44 so
LLM can refill it next round, or look it up manually (likely around
1,500-3,000).

</details>

### 5. APF-kring → umbrella aggregation (VERWORPEN 2026-07-31)

Dit punt stelde voor het vermogen van de kringen op te tellen naar de koepelrij. Dat moet juist niet: dan telt elke euro twee keer mee in het sectortotaal, precies waar de controle 'APF-moeder telt dubbel met zijn kringen' voor bestaat. Stap APF liet het live zien met 0,13 miljard op de koepel naast 8,30 miljard aan kringen; die 0,13 is opgeruimd.

De juiste modellering is de omgekeerde: de kringen dragen vermogen en deelnemers, de koepelrij blijft daarop leeg. Wie de omvang van een APF wil weten telt de kringen op — afgeleid, niet opgeslagen. De beleidsdekkingsgraad op de koepel mag wel blijven staan; een verhouding telt niet dubbel.

Stand: Hnp 10 kringen (7,8 mrd), Stap 11 (8,3), Centraal Beheer 15 (7,7), De Nationale 9 (3,7), Unilever 2 (6,0).

<details><summary>oorspronkelijke tekst</summary>


DNB reports HNPF (fund_id 64), DeNAPF (145), Centraal Beheer APF (65),
Stap APF (67), and Unilever APF (68) per-kring (e.g. "Kring Cargill (Hnp)").
Aggregating kring-level data back to the APF umbrella row would fill the
remaining AUM/beleidsdg NULLs for those 5 funds.

Approach: identify each umbrella's kringen via name pattern, sum kring
AUM, weighted-average kring beleidsdg by AUM share, write to the umbrella row.

</details>

### 6. SFDR / EU taxonomy gaps (MEDIUM value, HIGH effort)

After all our extraction passes:
- SFDR NULL: 75 funds (mostly without a local PDF, or with PDFs that don't mention an Article 6/8/9 classification).
- EU Taxonomy NULL: 132 funds (taxonomy reporting is recent and many small funds simply don't report it).

The remaining gaps need either bigger PDFs (download more FY2024 jaarverslagen from scraped_documents URLs) or a manual data entry pass.

### 7. Data-quality outlier detection (AFGEROND 2026-07-31)

Ingebouwd in `check_data_quality.py` als `uitschieters_jaarreeks`, met grenzen op zeven kolommen. Vond meteen Gasunie: toeslagen van 121 tot 137 procent over 2020-2024, die in werkelijkheid nominale dekkingsgraden waren en een kolom waren opgeschoven. Verplaatst; de controle staat op OK.

<details><summary>oorspronkelijke tekst</summary>


The Hoogovens-style "162% rendement" issue was found by `ABS(value) > 50`.
Add similar sanity sweeps periodically:

```sql
-- Outliers across historical_metrics
SELECT 'rendement >50%' AS issue, COUNT(*) FROM historical_metrics WHERE ABS(beleggingsrendement_pct) > 50
UNION SELECT 'beleidsdg outside 50..250', COUNT(*) FROM historical_metrics WHERE beleidsdekkingsgraad_pct NOT BETWEEN 50 AND 250
UNION SELECT 'aum <=0 or >1000 Bn', COUNT(*) FROM historical_metrics WHERE aum_euro_bn <=0 OR aum_euro_bn > 1000
;
```

</details>

### 8. Streamlit-aggrid for true click-to-detail table (LOW value, MEDIUM effort)

Sector Overview's Fund Directory uses Streamlit's native dataframe with
row selection. A nicer UX would be inline cell badges (category color,
status pill) which native dataframe can't render. `streamlit-aggrid` would
fix this but adds a dependency. Not done because the current UX works fine.

### 9. Jaarverslag-analyse: top-30 done (CLOSED — 0 FY0 remaining)

`fund_analysis` table holds 24 LLM-generated summaries:
- FY2025: 5 (9 ABP, 13 Beroepsvervoer, 32 PGB, 41 PFZW, 111 KPN)
- FY2024: 18 (incl. 3 Huisartsen, 5 SPMS, 16 BPL, 17 Detailhandel, 24 PMT,
  34 Recreatie, 71 ABN, 72 Achmea, 76 APG, 119 Philips, 123 Rabobank,
  145 NN, …)
- FY2023: 1 (38 PWRI)
- FY0: 0 ✅

All historical FY0 rows resolved on 2026-05-25 (commits `7cb2267`,
`3f09895`, `5dab47f`). Two funds had wrongly-set `funds.website`
(fid=3 sphn.nl → Haskoning DHV; fid=34 kikk-recreatie.nl → CAO-org)
which got fixed in the same pass.

**Reusable per-fund workflow** (template for future top-50 broadening):
1. Find the public PDF URL — start from the news-radar query in the
   "Sessie 2026-05-25" section; otherwise visit the fund's
   "annual reports" page; for JS-rendered sites use Playwright.
2. Save as `data/annual_reports/<fid>_<Name>_<YYYY>.pdf` — the naming
   pattern that `build_inventory` parses for the year.
3. `DELETE FROM fund_analysis WHERE fund_id=<id> AND fiscal_year=0;`
   (composite PK is fund_id+fiscal_year, so the FY0 row won't be
   overwritten by a new FY2024/FY2025 row — must drop manually).
4. `python3 scripts/document_parsing/llm_extract_analysis.py --funds <id>`
   (no --force needed after the delete).

Broaden to `--top 50` or beyond when bandwidth allows (currently 24
rows; the inventory has ~100 PDFs available).

**Known guardrails already in code (commit b011291 + this session):**
- `_is_toc_page` filter — skip pages where >30% of lines are bare
  digits (TOC signature).
- `sentence_score >= 2` required to enter the candidate pool.
- `_NON_JAARVERSLAG` filter — `build_inventory` excludes
  `*_Transitieplan.pdf`, `*_ESG.pdf`, `*_SFDR.pdf`, infographics. Without
  this filter the top-30 run picked `106_Hoogovens_Transitieplan.pdf`
  alphabetically over `106_Hoogovens.pdf`.

### 10. Post-invaren reporting template (AFGEROND 2026-07-31)

De blokkade is weg: de eerste postinvaren-jaarverslagen zijn verschenen. Het schema hieronder is doorgevoerd — `solidariteitsreserve_pct` en `collectief_pensioenvermogen_eur_bn` op `historical_metrics`, `invaardatum` en `invaardekkingsgraad_pct` op `funds`. PWRI staat op een solidariteitsreserve van 5,3 procent bij 11,06 miljard collectief vermogen, Loodsen op 7,5 procent.

`funds.invaardatum` is voor 33 fondsen afgeleid uit hun eigen transitieparagraaf. Daarmee kan `check_data_quality.py` nu vaststellen of een lege dekkingsgraad een gat is of juist correct — voor een ingevaren fonds is die leegte de bedoeling.

Beide resterende stukken zijn nu ook gedaan. De grafiek markeert het invaarjaar met een stippellijn en toont de solidariteitsreserve zodra die gevuld is. En `cohort_metrics(fund_id, year, cohort_label, profiel, rendement_pct, bron)` bestaat, gevuld met 65 waarnemingen van StiPP over 2021-2025.

Die reeks laat zien waar het bij de Wtp om draait. Over 2025 haalde de groep 18-46 met een standaard profiel 7,8 procent en de groep 65-66 juist -2,4 procent; hetzelfde fonds, hetzelfde jaar. Opvallend is 2022: toen verloor de oudste groep 23,7 procent tegen 14,9 procent voor de jongste. Dat is de omgekeerde volgorde van wat je bij een lifecycle verwacht, en het komt doordat oudere cohorten meer rentegevoelig belegd zijn — precies het risico dat in dat jaar losbarstte.

Wat niet lukt is automatisch uitlezen. StiPP zet deze reeks ook als staafdiagram op de pagina, en daar zweven de waarden los van hun labels; alleen doordat de platgeslagen tekst de reeks op volgorde bevat was het te herleiden. Bij een fonds dat het uitsluitend als grafiek publiceert, is handwerk nodig.

<details><summary>oorspronkelijk ontwerp</summary>


Eenmaal een fonds is ingevaren onder Wtp **verdwijnt de dekkingsgraad
uit het jaarverslag**. Onze huidige `historical_metrics`-schema is
volledig dekkingsgraad-georiënteerd en zal NULL-rijen produceren voor
invaarders vanaf hun FY2025. Voor de 6 fondsen die per 1-1-2025 zijn
ingevaren (4 Loodsen, 29 POB, 30 Particuliere Beveiliging, 38 PWRI,
76 APG, 193 Kring Van Lanschot HNPF) is FY2025 = eerste post-invaren
jaarverslag; verwacht in zomer/herfst 2026.

**Template afgeleid uit PWRI + Loodsen FY2024** (laatste pre-invaren
year — bevat forward-looking sectie over wat FY2025+ rapporteert):

| Oud (DB, t/m FY2024) | Nieuw (Wtp, vanaf FY2025) |
|---|---|
| Actuele / beleids / reële / vereiste / minimaal vereiste dekkingsgraad | **Persoonlijk pensioenvermogen** per deelnemer |
| Voorziening pensioenverplichtingen | **Collectief belegd pensioenvermogen** |
| Toeslagverlening (jaarlijks bij beleidsdg ≥ 110%) | Pensioen beweegt direct mee met economie / **beleggingsrendement** |
| Premiedekkingsgraad (voor herstelplannen) | **Beleggingsrendement per cohort / leeftijdsfase** |
| Vermogen - Voorziening = Buffer | **Solidariteitsreserve** (solidair contract; Loodsen start 7,5%) of **Risicodelingsreserve** (flexibel contract) |
| — | **Netto profijt** per leeftijdscohort (evenwichtigheidsmetric) |
| — | **Invaardekkingsgraad** (Loodsen: 119%) — eenmalig bij overgang |

Contract-types in `funds.wtp_contract_type` bepalen welke variant:
`solidair` / `SPR` → solidariteitsreserve;
`flexibel` / `FPR` / `flexibel + rdr` → individuele beleggingsmix +
risicodelingsreserve.

**Schema-implicaties voor `historical_metrics`** (te beslissen vóór de
eerste FY2025 verschijnt):
- `dekkingsgraad_*`-kolommen krijgen NULL voor invaarders vanaf hun
  FY-invaren — niet als bug behandelen, gewoon legitiem NULL.
- Nieuwe kolommen voorstel: `solidariteitsreserve_pct`,
  `invaardekkingsgraad_pct` (eenmalig per fund, zou óók in `funds`
  kunnen), `collectief_pensioenvermogen_eur_bn`.
- `beleggingsrendement_per_cohort` past niet in een wide table —
  vermoedelijk een nieuwe `cohort_metrics(fund_id, year, cohort_label,
  rendement_pct)` tabel.
- Dashboard moet een toggle krijgen: voor ingevaren funds toon je
  beleggingsrendement-grafiek; voor niet-ingevaren toon je
  dekkingsgraad-grafiek.

**Monitoring (sessie 2026-05-26):** Een scheduled remote agent
`trig_014uegtLHMZLJ55qVy4PuGPm` ("FY2025 invaarder jaarverslag radar")
draait one-shot op 2026-06-15 06:00 UTC. Hij gebruikt **drie parallelle
signalen** — news-only bleek onbetrouwbaar nadat audit liet zien dat
4 van de 6 invaarders (Loodsen, POB, Particuliere Beveiliging, APG)
nooit jaarverslag-aankondigingen via news plaatsen.

De drie signalen:
1. `news_articles` titel matched `%jaarverslag%2025%`/`%2025 is klaar%`
   etc., gepubliceerd ná 2026-05-25, voor fund_id IN (4,29,30,38,76,193).
2. `scraped_documents` URL of titel matched `%jaarverslag-2025%` /
   `%jaarverslag_2025%` etc., voor dezelfde fund_ids. Robuuster — de
   bi-daily scraper kent al de URL-patronen voor Loodsen/PWRI/APG.
3. Direct HEAD-check op drie bekende URL-patronen:
   - `pwri.nl/.../jaarverslag-2025-pwri.pdf`
   - `bploodsen.nl/.../jaarverslag-2025.pdf` (+ verkort-variant)

Status op 2026-05-26 (alle drie lokaal getest): allemaal leeg / 404.

**First steps when first FY2025 invaarder-jaarverslag landt:**
1. Routine fires automatisch 2026-06-15; output op
   `https://claude.ai/code/routines/trig_014uegtLHMZLJ55qVy4PuGPm`.
   Niets gevonden → routine re-armen voor +3 weken.
2. Bij eerste hit: PDF downloaden + handmatig inspecteren wat er
   feitelijk in de kerncijfers-tabel staat.
3. Bevestigt het template hierboven? Zo ja: schema-migratie schrijven.
4. LLM-extractor `llm_extract_analysis.py` aanvullen met patroon-
   herkenning voor de nieuwe metrics (vooral solidariteitsreserve %
   en invaardekkingsgraad zijn relatief vast format).

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

</details>
