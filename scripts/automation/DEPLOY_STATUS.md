# Deploy-status — pensioenfondsen.webkowuite.nl

_Laatst bijgewerkt: 2026-06-10. Handoff zodat het werk op de Mac mini verder kan._

## Doel
Het Streamlit-dashboard draaien op het eigen domein **https://pensioenfondsen.webkowuite.nl**,
gehost op de **Mac mini** ("Mac mini van Webko", Apple M4, user `webkowuite`),
via de **bestaande** Cloudflare-tunnel. **Status: LIVE** (extern 200, interacties werken).

## Wat draait op de Mac mini  ✅
| Onderdeel | Detail |
|---|---|
| Clone | `~/pensioenfondsen-app` (git, publieke repo `woyzeck1978/pensioenfondsen`, trackt `main`) |
| Venv | `~/pensioenfondsen-app/.venv` — **Python 3.9.6, Streamlit 1.50.0**. Code moet 3.9-syntax-proof zijn (`from __future__ import annotations` bij `X \| None`), maar moderne Streamlit-API's (st.navigation, st.context.theme, segmented_control) werken hier gewoon |
| Dashboard-service | launchd `nl.wuite.pensioenfondsen.dashboard` → Streamlit op **`0.0.0.0:8502`** onder `caffeinate -i`, KeepAlive |
| Pull-timer | launchd `nl.wuite.pensioenfondsen.pull` → dagelijks **06:30** via `~/bin/pensioenfondsen_pull.sh`: ff-only pull, dashboard-herstart alleen bij nieuwe commit, daarna `generate_alerts.py --all-funds` |
| Scrape-job | launchd `nl.wuite.pensioenfondsen.scrape` → dagelijks **06:00** `scripts/automation/scrape_mini.sh` (scrape → push naar GitHub → herstart + alerts bij nieuwe commit). MBP-job is uitgezet (10-6) |
| API-sidecar | launchd `nl.wuite.pensioenfondsen.api` → FastAPI op **0.0.0.0:8503**, Tailscale-only (niet via de tunnel). Leest live uit de DB; alleen herstarten bij code-wijziging |
| Health | loopback `127.0.0.1:8502` = 200, tailnet `100.107.33.80:8502` = 200, extern = 200 |
| Coëxistentie | bestaand `energie`-dashboard op 8501 ongemoeid |

## Bestaande tunnel — NIET vervangen
- launchd `com.webkowuite.cloudflared` (token-based, **dashboard-managed**),
  tunnel-UUID `52bfb54f-c9b6-42c1-b56d-53b47c87e3d2`.
- Ingress/Public-Hostnames worden beheerd in **Cloudflare Zero Trust**
  (one.dash.cloudflare.com → Networks → Tunnels), **niet** in een lokale `config.yml`.
- Origin staat op het **tailnet-IP** `100.107.33.80:8502` (NIET 127.0.0.1 — de
  tunnel heeft meerdere connectors; loopback gaf 502 vanaf elke niet-mini-connector).
- `--protocol http2` is uit de plist verwijderd (default quic) — http2 brak
  Streamlit's WebSocket-runs (pagina rendert, maar knoppen/forms "doen niets").
- ⚠️ De tunnel-token stond in een eerdere chat zichtbaar; overweeg te roteren als dat transcript gedeeld wordt.

## UI-facelift (2026-06-10, commit `a70c20b`)
Grote UI-verbouwing van `scripts/utils_and_viz/dashboard.py` + `style.css`
(+ nieuw `style_dark.css`):
- **`st.navigation`** met functie-pagina's: gegroepeerde sidebar (Overzicht /
  Fondsen / Analyse / Transitie & ESG / Hulpmiddelen) en echte URL's per pagina
  (`/fonds`, `/wtp`, …). Oude `/?fund=`-links redirecten. Cross-page jumps via
  pending-jump in session_state (st.switch_page mag niet in widget-callbacks).
- **Diepteanalyse**: fondsheader met badges + vier tabs (kerncijfers/historie/
  ESG/documenten), deelnemers-donut.
- **Grafieken**: AUM-treemap i.p.v. pie, vaste categoriekleuren overal
  (`CATEGORY_COLORS`), horizontale bars voor beheerders/contracttypes,
  DNB-kwartaaldelta op de dekkingsgraad-KPI.
- **Dark mode**: detectie via `st.context.theme` (try/except-fallback licht),
  `style_dark.css` + `csu_dark` Plotly-template. Werkt op deze mini (1.50).
- Hoofdquery gecached (ttl 300s; cache wordt geleegd bij Datacuratie-promotie).

## Nieuwe versie uitrollen + verifiëren
```bash
# 1. Pull + herstart (alleen bij nieuwe commit) + alerts:
~/bin/pensioenfondsen_pull.sh && tail -5 ~/pensioenfondsen-app/logs/server/pull.log

# 2. Commit + health:
git -C ~/pensioenfondsen-app log --oneline -1
curl -s http://127.0.0.1:8502/_stcore/health        # → ok
curl -s -o /dev/null -w '%{http_code}\n' https://pensioenfondsen.webkowuite.nl/

# 3. Grondig — AppTest-smoke van alle pagina's in deze venv. AppTest kan niet
#    switch_page'n naar functie-pagina's: monkeypatch st.navigation met een stub
#    die de doel-functie (page._page) in run() aanroept, en loop zo elke
#    url_path af (None, fonds, vergelijk, trends, nieuws, dekkingsgraad,
#    aandelenstrategie, beheerders, wtp, esg, begrippen, vraag, curatie).
```
**Streamlit Cloud** (pensioenfondsen.streamlit.app) rebuildt zelf vanaf main,
maar is **niet met curl te verifiëren** (elke route geeft de SPA-shell, 200).
Check met Playwright: app kan hiberneren ("Yes, get this app back up" klikken,
daarna minuten wachten op boot/dep-install), dan markers zoeken ("Nederlandse
pensioenfondsen", nav-secties) of een screenshot nemen. NB: de sidebar-merkregel
is CSS-`::before`-content en dus onzichtbaar voor `inner_text`.

## Beheer
```bash
launchctl list | grep pensioen
tail -f ~/pensioenfondsen-app/logs/server/dashboard.err.log
tail -f ~/pensioenfondsen-app/logs/server/pull.log
launchctl kickstart gui/$(id -u)/nl.wuite.pensioenfondsen.pull   # data nu verversen
git -C ~/pensioenfondsen-app pull --ff-only                      # handmatig verversen
```

## Setup-script (idempotent, herbruikbaar)
`scripts/automation/setup_server.sh` (sinds 2026-06-10 ook op GitHub, commit `ff3c675`). Detecteert vrije poort + bestaande tunnel, bindt `0.0.0.0`,
schrijft de launchd-services + pull-timer, en print de juiste CF-instructie
(met tailnet-IP). Voor een **HA-replica op de M1 MBP**: zelfde script daar draaien
en in CF een tweede origin/replica toevoegen.

## Openstaand / mogelijke vervolgstappen
- [x] CF Public Hostname op `100.107.33.80:8502` — gedaan 2026-06-05, extern 200.
- [x] Streamlit-gedrag achter proxy — opgelost 2026-06-09 (http2 → quic).
- [x] UI-facelift uitgerold en op alle drie de deployments geverifieerd (2026-06-10).
- [x] `setup_server.sh` + dit document naar GitHub gecommit (`ff3c675`, 2026-06-10).
- [ ] Optioneel: HA-replica op M1 MBP.
- [ ] cloudflared is verouderd (2026.3.0 → 2026.5.2); upgrade raakt álle sites, dus apart inplannen.
