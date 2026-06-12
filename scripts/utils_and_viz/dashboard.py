from __future__ import annotations  # 3.9-deploy safe: defer `str | None` etc.
import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.io as pio
import os
import urllib.parse
import re
import html as _html
import io
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# Writable companion state (overrides / watchlist / audit). Only meaningful on
# the self-hosted deployment; see local_state.py. Import is defensive so the
# dashboard still loads if the module is ever absent.
try:
    import local_state
    local_state.init_state_db()
    _LOCAL_STATE_OK = True
except Exception as _ls_err:  # pragma: no cover - defensive
    local_state = None
    _LOCAL_STATE_OK = False

# Live text-to-SQL via local Ollama (Fase 3). Defensive import — absence just
# hides the "Vraag het de data" page.
try:
    import text2sql
    _TEXT2SQL_OK = True
except Exception:  # pragma: no cover - defensive
    text2sql = None
    _TEXT2SQL_OK = False

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Dutch Pension Funds Explorer",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- THEME DETECTION + IN-APP DARK MODE SWITCH ---
def _system_dark() -> bool:
    """True wanneer Streamlit zelf (instellingenmenu/OS) in dark mode staat.

    st.context.theme bestaat pas sinds Streamlit 1.46; op oudere runtimes
    (zoals de Python 3.9-deploy op de mini) valt dit terug op licht.
    """
    try:
        return st.context.theme.type == "dark"
    except Exception:
        return False

_SYSTEM_DARK = _system_dark()
# Eerste bezoek volgt het Streamlit/OS-thema; daarna is de sidebar-toggle
# (key="dark_mode", gerenderd verderop) leidend voor deze sessie.
if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = _SYSTEM_DARK
IS_DARK = bool(st.session_state.dark_mode)

# --- VISUAL STYLE (inject style.css [+ dark overrides] next to this file) ---
# style_dark_native.css dekt native widgets (inputs, dropdowns, canvas-
# tabellen) en is alleen nodig als de toggle donker staat terwijl Streamlit
# zelf licht gethemed is — anders themed Streamlit die al zelf.
_STYLE_DIR = os.path.dirname(os.path.abspath(__file__))
_css_files = ["style.css"]
if IS_DARK:
    _css_files.append("style_dark.css")
    if not _SYSTEM_DARK:
        _css_files.append("style_dark_native.css")
_css = ""
for _name in _css_files:
    _p = os.path.join(_STYLE_DIR, _name)
    if os.path.exists(_p):
        with open(_p) as _f:
            _css += _f.read() + "\n"
if _css:
    st.markdown(f"<style>{_css}</style>", unsafe_allow_html=True)

def kpi_card(label: str, value: str, sub: str = "", delta: str | None = None, delta_dir: str = "up") -> str:
    """Return HTML for a CSU-style KPI tile. Use inside a kpi-row grid."""
    delta_html = ""
    if delta:
        cls = "kpi-delta-up" if delta_dir == "up" else "kpi-delta-down"
        delta_html = f' <span class="{cls}">{_html.escape(delta)}</span>'
    sub_html = f'<div class="kpi-sub">{_html.escape(sub)}{delta_html}</div>' if (sub or delta) else ""
    return (
        '<div class="kpi-card">'
        f'<div class="kpi-label">{_html.escape(label)}</div>'
        f'<div class="kpi-value">{_html.escape(value)}</div>'
        f'{sub_html}'
        '</div>'
    )

def render_kpi_row(cards: list[str]) -> None:
    st.markdown('<div class="kpi-row">' + "".join(cards) + '</div>', unsafe_allow_html=True)

def badge(text: str, color: str = "gray") -> str:
    """Return HTML for a pill badge. color in {green,teal,blue,purple,orange,red,gray,outline}."""
    return f'<span class="badge badge-{color}">{_html.escape(text)}</span>'

# Plotly defaults aligned with the CSU palette (licht + donker)
import copy as _copy

_PLOT_FONT = ('"IBM Plex Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", '
              '"Helvetica Neue", Arial, sans-serif')

pio.templates["csu_light"] = _copy.deepcopy(pio.templates["simple_white"])
pio.templates["csu_light"].layout.update(
    font=dict(family=_PLOT_FONT, color="#17202A", size=12),
    colorway=["#6554A3", "#1F6FB2", "#17756B", "#2F7D57", "#C66B16", "#B13B3B", "#8F9BA8"],
    paper_bgcolor="#FFFFFF",
    plot_bgcolor="#FFFFFF",
    margin=dict(l=10, r=10, t=40, b=10),
    title=dict(font=dict(size=13, color="#5C6875")),
    legend=dict(font=dict(size=11)),
)

pio.templates["csu_dark"] = _copy.deepcopy(pio.templates["simple_white"])
pio.templates["csu_dark"].layout.update(
    font=dict(family=_PLOT_FONT, color="#E4E7EC", size=12),
    colorway=["#9D8FD9", "#6FA8DC", "#5BB8AC", "#6FBF8F", "#E09A4C", "#D97070", "#9AA7B5"],
    paper_bgcolor="#171C24",
    plot_bgcolor="#171C24",
    margin=dict(l=10, r=10, t=40, b=10),
    title=dict(font=dict(size=13, color="#9AA7B5")),
    legend=dict(font=dict(size=11)),
    xaxis=dict(gridcolor="#2A323D", linecolor="#3A4452", zerolinecolor="#3A4452"),
    yaxis=dict(gridcolor="#2A323D", linecolor="#3A4452", zerolinecolor="#3A4452"),
)
pio.templates.default = "csu_dark" if IS_DARK else "csu_light"

# Vaste kleuren per fondscategorie — identiek op elke pagina, zodat 'Bedrijf'
# overal dezelfde kleur heeft. Sleutels = funds.category in de DB.
if IS_DARK:
    CATEGORY_COLORS = {
        "Bedrijf": "#6FA8DC", "Tak": "#9D8FD9", "APF": "#5BB8AC",
        "Beroep": "#6FBF8F", "Verzekeraar": "#E09A4C", "PPI": "#9AA7B5",
        "Algemeen Pensioenfonds (Kring)": "#5BB8AC",
        "APF (AkzoNobel, Nouryon, Nobian, Salt)": "#5BB8AC",
        "Onbekend": "#6B7684",
    }
else:
    CATEGORY_COLORS = {
        "Bedrijf": "#1F6FB2", "Tak": "#6554A3", "APF": "#17756B",
        "Beroep": "#2F7D57", "Verzekeraar": "#C66B16", "PPI": "#8F9BA8",
        "Algemeen Pensioenfonds (Kring)": "#17756B",
        "APF (AkzoNobel, Nouryon, Nobian, Salt)": "#17756B",
        "Onbekend": "#B8C0CA",
    }

# Badge-kleurnaam per benchmark-provider (funds.benchmark_providers,
# afgeleid door db_management/derive_benchmark_providers.py)
PROVIDER_BADGE = {
    "MSCI": "purple",
    "Bloomberg (Barclays)": "blue",
    "FTSE Russell": "teal",
    "GRESB (vastgoed-ESG)": "green",
    "J.P. Morgan": "orange",
    "S&P Dow Jones (incl. iBoxx)": "gray",
}

# Badge-kleurnaam per categorie (voor de pill-badges in previews/vergelijking)
CATEGORY_BADGE = {
    "Tak": "purple", "Bedrijf": "blue", "Beroep": "green",
    "Verzekeraar": "orange", "APF": "teal", "PPI": "gray",
    "Algemeen Pensioenfonds (Kring)": "teal",
    "APF (AkzoNobel, Nouryon, Nobian, Salt)": "teal",
}

GRID_LINE = "#2A323D" if IS_DARK else "#D9DEE5"
MUTE_LINE = "#9AA7B5" if IS_DARK else "#5C6875"
RED_LINE = "#D97070" if IS_DARK else "#B13B3B"
ACCENT = "#9D8FD9" if IS_DARK else "#6554A3"
GREEN_FILL = "#6FBF8F" if IS_DARK else "#2F7D57"
ORANGE_FILL = "#E09A4C" if IS_DARK else "#C66B16"
BLUE_FILL = "#6FA8DC" if IS_DARK else "#1F6FB2"
CHART_BG = "#171C24" if IS_DARK else "#FFFFFF"
CHART_FG = "#E4E7EC" if IS_DARK else "#17202A"


def show_chart(fig, **kwargs):
    """st.plotly_chart-wrapper met expliciete achtergrond-/tekstkleuren.

    Streamlit's frontend synchroniseert chart-achtergronden met het eigen
    (lichte) app-thema, óók bij theme=None — maar alleen voor waarden die
    de figuur niet expliciet zet. Expliciet zetten laat de csu-templates
    dus winnen, en daarmee werkt de in-app dark-mode-toggle ook op charts.
    """
    fig.update_layout(paper_bgcolor=CHART_BG, plot_bgcolor=CHART_BG,
                      font_color=CHART_FG)
    kwargs.setdefault("use_container_width", True)
    kwargs.setdefault("theme", None)
    st.plotly_chart(fig, **kwargs)

# --- DATABASE CONNECTION ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

def load_data(query):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# --- DATA FETCHING ---
@st.cache_data(ttl=300)
def get_all_funds():
    query = """
    SELECT 
        f.id, f.name, f.category, f.aum_euro_bn, 
        COALESCE(f.maanddekkingsgraad_pct, f.dekkingsgraad_pct) AS dekkingsgraad_pct, 
        f.beleidsdekkingsgraad_pct,
        f.equity_allocation_pct, f.uitvoerder, f.deelnemers_totaal, f.website,
        f.deelnemers_actief, f.deelnemers_slapers, f.deelnemers_gepensioneerd,
        f.sfdr_article, f.eu_taxonomy_pct, f.investment_beliefs,
        f.benchmark_providers, f.benchmark_providers_bron, f.benchmarks,
        e.co2_reduction_goal, e.sfdr_classification
    FROM funds f
    LEFT JOIN fund_esg_metrics e ON f.id = e.fund_id
    """
    return load_data(query)

@st.cache_data(ttl=3600)
def get_dnb_sector_bdg():
    """Sectorgemiddelde DNB-beleidsdekkingsgraad, laatste twee kwartalen.

    Voedt het delta-pijltje op de KPI-card van het sectoroverzicht.
    """
    q = """
    SELECT year, quarter, AVG(value) AS avg_bdg, COUNT(*) AS n
    FROM dnb_quarterly_metrics
    WHERE metric_name = 'Beleidsdekkingsgraad' AND value IS NOT NULL
    GROUP BY year, quarter
    ORDER BY year DESC, quarter DESC
    LIMIT 2
    """
    try:
        return load_data(q)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def get_latest_news():
    query = """
    -- Only show articles with a real publication date. The scraped_documents
    -- fallback used discovered_at (=scrape date), which conflated "added to
    -- DB today" with "published today". Items without a parsed publication
    -- date are filtered out rather than shown with a misleading scrape date.
    SELECT
        n.published_date as "Date",
        f.name as "Pension Fund",
        n.title as "Headline",
        n.url,
        f.category as "Category"
    FROM news_articles n
    JOIN funds f ON n.fund_id = f.id
    WHERE n.published_date IS NOT NULL
      AND n.title IS NOT NULL
    ORDER BY date(n.published_date) DESC, n.id DESC
    LIMIT 500
    """
    return load_data(query)

def build_fund_factsheet_pdf(fund_row, history_df) -> bytes:
    """Return a 1-page PDF factsheet as bytes. Uses matplotlib only."""
    buf = io.BytesIO()
    with PdfPages(buf) as pdf:
        fig = plt.figure(figsize=(8.27, 11.69), dpi=120)  # A4 portrait
        gs = fig.add_gridspec(6, 2, hspace=0.55, wspace=0.25)

        # Header
        ax_head = fig.add_subplot(gs[0, :]); ax_head.axis("off")
        ax_head.text(0.0, 0.85, str(fund_row.get('name') or 'Fund'),
                     fontsize=18, fontweight='bold', color='#17202A')
        cat = fund_row.get('category') or '—'
        ax_head.text(0.0, 0.55, f"Category: {cat}", fontsize=10, color='#5C6875')
        ax_head.text(1.0, 0.85, "Pension Fund Factsheet", fontsize=10,
                     color='#5C6875', ha='right')
        ax_head.text(1.0, 0.55, f"Generated {pd.Timestamp.now(tz='UTC').tz_localize(None).date()}",
                     fontsize=8, color='#8F9BA8', ha='right')

        # KPI tiles
        ax_kpi = fig.add_subplot(gs[1, :]); ax_kpi.axis("off")
        def _fmt(v, fmt):
            if v is None or pd.isna(v):
                return "—"
            return fmt.format(v)
        kpis = [
            ("AUM",                _fmt(fund_row.get('aum_euro_bn'),         "€{:,.1f} Bn")),
            ("Dekkingsgraad",      _fmt(fund_row.get('dekkingsgraad_pct'),   "{:.1f}%")),
            ("Beleidsdekkingsgr.", _fmt(fund_row.get('beleidsdekkingsgraad_pct'), "{:.1f}%")),
            ("Equity allocation",  _fmt(fund_row.get('equity_allocation_pct'), "{:.1f}%")),
            ("Deelnemers totaal",  _fmt(fund_row.get('deelnemers_totaal'),   "{:,.0f}").replace(",", ".")),
        ]
        for i, (label, value) in enumerate(kpis):
            x = 0.02 + i * 0.196
            ax_kpi.text(x, 0.70, label.upper(), fontsize=7.5,
                        color='#5C6875', transform=ax_kpi.transAxes)
            ax_kpi.text(x, 0.30, value, fontsize=14, fontweight='bold',
                        color='#17202A', transform=ax_kpi.transAxes)

        # Beleidsdekkingsgraad over time
        ax_dekk = fig.add_subplot(gs[2:4, :])
        if not history_df.empty and history_df['beleidsdekkingsgraad_pct'].notna().any():
            d = history_df.sort_values('year')
            ax_dekk.plot(d['year'], d['beleidsdekkingsgraad_pct'],
                         marker='o', color='#6554A3', linewidth=1.8)
            ax_dekk.axhline(100, linestyle='--', color='#B13B3B', linewidth=0.8)
            ax_dekk.set_title("Beleidsdekkingsgraad (%)", fontsize=11, loc='left', color='#2F3A45')
            ax_dekk.set_xlabel(""); ax_dekk.set_ylabel("")
            ax_dekk.spines[['top', 'right']].set_visible(False)
            ax_dekk.grid(True, axis='y', linestyle=':', linewidth=0.5, color='#D9DEE5')
        else:
            ax_dekk.axis("off")
            ax_dekk.text(0.5, 0.5, "No history available", ha='center', color='#8F9BA8')

        # AUM evolution
        ax_aum = fig.add_subplot(gs[4, 0])
        if not history_df.empty and history_df['aum_euro_bn'].notna().any():
            d = history_df.sort_values('year')
            ax_aum.bar(d['year'], d['aum_euro_bn'].fillna(0), color='#1F6FB2', width=0.7)
            ax_aum.set_title("AUM (€ Bn)", fontsize=10, loc='left', color='#2F3A45')
            ax_aum.spines[['top', 'right']].set_visible(False)
            ax_aum.grid(True, axis='y', linestyle=':', linewidth=0.5, color='#D9DEE5')
        else:
            ax_aum.axis("off")

        # Deelnemers stacked
        ax_dln = fig.add_subplot(gs[4, 1])
        cols = ['deelnemers_actief', 'deelnemers_slapers', 'deelnemers_pensioengerechtigd']
        if not history_df.empty and any(c in history_df.columns and history_df[c].notna().any() for c in cols):
            d = history_df.sort_values('year').copy()
            for c in cols:
                if c not in d.columns:
                    d[c] = 0
            d[cols] = d[cols].fillna(0)
            ax_dln.stackplot(d['year'],
                             d['deelnemers_actief'], d['deelnemers_slapers'],
                             d['deelnemers_pensioengerechtigd'],
                             labels=['actief', 'slapers', 'gepens.'],
                             colors=['#2F7D57', '#C66B16', '#6554A3'], alpha=0.85)
            ax_dln.set_title("Deelnemers", fontsize=10, loc='left', color='#2F3A45')
            ax_dln.legend(loc='upper left', fontsize=7, frameon=False)
            ax_dln.spines[['top', 'right']].set_visible(False)
        else:
            ax_dln.axis("off")

        # Footer
        ax_foot = fig.add_subplot(gs[5, :]); ax_foot.axis("off")
        uitv = fund_row.get('uitvoerder') or '—'
        sfdr_raw = fund_row.get('sfdr_article')
        sfdr = f"Article {int(sfdr_raw)}" if pd.notnull(sfdr_raw) else "—"
        site = fund_row.get('website') or '—'
        ax_foot.text(0.0, 0.85, f"Uitvoerder: {uitv}", fontsize=9, color='#2F3A45')
        ax_foot.text(0.0, 0.55, f"SFDR: {sfdr}", fontsize=9, color='#2F3A45')
        ax_foot.text(0.0, 0.25, f"Website: {site}", fontsize=8, color='#5C6875')
        ax_foot.text(1.0, 0.10, "Source: DNB statpub + funds' annual reports",
                     fontsize=7, color='#8F9BA8', ha='right')

        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def get_peer_history(category: str, exclude_fund_id: int):
    """Per-year sector average for funds in the same category, excluding self.

    Returns columns: year, peer_beleidsdg, peer_rendement, peer_count.
    """
    if not category:
        return pd.DataFrame()
    q = f"""
    SELECT h.year,
           AVG(h.beleidsdekkingsgraad_pct) AS peer_beleidsdg,
           AVG(h.beleggingsrendement_pct)  AS peer_rendement,
           COUNT(DISTINCT h.fund_id)       AS peer_count
    FROM historical_metrics h JOIN funds f ON f.id = h.fund_id
    WHERE f.category = ? AND h.fund_id != ?
    GROUP BY h.year
    ORDER BY h.year
    """
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(q, con, params=(category, exclude_fund_id))
    con.close()
    return df


def get_metrics_history(fund_id):
    """Multi-year metrics for a fund.

    Primary source is historical_metrics (rich, curated). Falls back to
    fy_annual_metrics for fiscal years that aren't yet in historical_metrics
    — so freshly-parsed jaarverslag values (e.g. FY2025 for KPN) appear on
    the chart without needing a separate backfill step.
    """
    query_main = f"""
    SELECT year, aum_euro_bn, economische_dekkingsgraad_pct, nominale_dekkingsgraad_pct,
           beleidsdekkingsgraad_pct, vereiste_dekkingsgraad_pct, reele_dekkingsgraad_pct,
           beleggingsrendement_pct, indexatieverlening_pct, cpi_pct,
           zakelijke_waarden_pct, rente_afdekking_pct, rente_afdekking_rendement_pct,
           deelnemers_actief, deelnemers_slapers, deelnemers_pensioengerechtigd, deelnemers_totaal
    FROM historical_metrics
    WHERE fund_id = {fund_id}
    """
    df_hist = load_data(query_main)

    # Pivot fy_annual_metrics for years not yet present
    query_fy = f"""
    SELECT fiscal_year AS year, metric_name, value
    FROM fy_annual_metrics
    WHERE fund_id = {fund_id}
      AND value IS NOT NULL
    """
    try:
        df_fy = load_data(query_fy)
    except Exception:
        df_fy = pd.DataFrame()

    if not df_fy.empty:
        present_years = set(df_hist['year'].tolist()) if not df_hist.empty else set()
        df_fy = df_fy[~df_fy['year'].isin(present_years)]
        if not df_fy.empty:
            metric_to_col = {
                "aum_eur_bn": "aum_euro_bn",
                "actuele_dekkingsgraad_pct": "economische_dekkingsgraad_pct",
                "beleidsdekkingsgraad_pct": "beleidsdekkingsgraad_pct",
                "reele_dekkingsgraad_pct": "reele_dekkingsgraad_pct",
                "beleggingsrendement_pct": "beleggingsrendement_pct",
            }
            df_fy = df_fy[df_fy['metric_name'].isin(metric_to_col)].copy()
            df_fy['col'] = df_fy['metric_name'].map(metric_to_col)
            wide = df_fy.pivot_table(index='year', columns='col', values='value', aggfunc='first').reset_index()
            wide.columns.name = None
            df_hist = pd.concat([df_hist, wide], ignore_index=True)

    df_hist = df_hist.sort_values('year').reset_index(drop=True)
    return df_hist

def get_fund_managers(fund_id):
    query = f"""
    SELECT fund_name as manager
    FROM equity_portfolio_funds
    WHERE fund_id = {fund_id}
    """
    return load_data(query)
    
def get_fund_news(fund_id):
    query = f"""
    SELECT published_date, title, url
    FROM news_articles
    WHERE fund_id = {fund_id}
    ORDER BY published_date DESC
    """
    return load_data(query)

def get_fund_reports(fund_id):
    """Top-5 annual report PDFs for a fund, newest fiscal year first.

    Year is extracted via regex on the title (e.g. 'Jaarverslag 2025.pdf'
    -> 2025) rather than slicing the last 4 chars, which produced '5.pd' and
    broke the sort.
    """
    query = f"""
    SELECT title, url
    FROM scraped_documents
    WHERE fund_id = {fund_id}
      AND doc_type = 'document'
      AND (lower(title) LIKE '%jaarverslag%'
           OR lower(title) LIKE '%jaarrapport%'
           OR lower(title) LIKE '%annual report%')
      AND lower(title) NOT LIKE '%maatschappelijk%'
      AND lower(title) NOT LIKE '%duurzaam%'
      AND lower(title) NOT LIKE '%esg%'
    """
    df = load_data(query)
    if df.empty:
        return df
    df['year_extracted'] = df['title'].str.extract(r'(20\d{2})').astype('Int64')
    df['is_verkort'] = df['title'].str.lower().str.contains('verkort', na=False)
    # Sort: newest year first; within a year, full report (not 'verkort') first
    df = df.sort_values(
        ['year_extracted', 'is_verkort', 'title'],
        ascending=[False, True, False],
        na_position='last',
    ).drop(columns=['is_verkort'])
    return df.head(5)


def get_fund_annual_metrics(fund_id):
    """Parsed metrics from annual-report PDFs (fy_annual_metrics)."""
    query = f"""
    SELECT fiscal_year, metric_name, value, source_url, notes
    FROM fy_annual_metrics
    WHERE fund_id = {fund_id}
    ORDER BY fiscal_year DESC, metric_name
    """
    try:
        return load_data(query)
    except Exception:
        return pd.DataFrame()

def get_fund_esg_reports(fund_id):
    query = f"""
    SELECT title, url
    FROM scraped_documents
    WHERE fund_id = {fund_id} 
      AND doc_type = 'document' 
      AND (lower(title) LIKE '%duurzaam%' OR lower(title) LIKE '%esg%' OR lower(title) LIKE '%maatschappelijk%' OR lower(title) LIKE '%mvo%')
    ORDER BY title DESC
    LIMIT 5
    """
    return load_data(query)

# --- MAIN APP LAYOUT ---
# Retrieve core dataset
df_funds = get_all_funds()
# Layer manual corrections on top of the bron-DB at read time (Fase 0). The
# bron-DB itself is never mutated by the dashboard, so the bi-daily scraper and
# git push stay conflict-free.
if _LOCAL_STATE_OK:
    df_funds = local_state.apply_overrides(df_funds, id_col="id")

# Session State Initialization
if "selected_fund" not in st.session_state:
    st.session_state.selected_fund = None

if "recent_funds" not in st.session_state:
    st.session_state.recent_funds = []  # most-recent first, max 5

# Watchlist (Fase 2): persisted in app_state.db, keyed on stable fund_id. The
# UI still works in fund *names*, so we keep name<->id maps and sync the
# session copy from the DB on every run. On a non-self-hosted deploy it falls
# back to the old session-only behaviour.
_NAME_TO_ID = dict(zip(df_funds["name"], df_funds["id"]))
_ID_TO_NAME = dict(zip(df_funds["id"], df_funds["name"]))

if _LOCAL_STATE_OK:
    _watch_ids = local_state.list_watchlist()
    st.session_state.watchlist = [
        _ID_TO_NAME[i] for i in _watch_ids if i in _ID_TO_NAME
    ]
elif "watchlist" not in st.session_state:
    st.session_state.watchlist = []  # pinned fund names, max 10


def _toggle_watch(fund_name: str) -> None:
    """Add or remove from watchlist (persisted when self-hosted)."""
    if _LOCAL_STATE_OK and fund_name in _NAME_TO_ID:
        local_state.toggle_watch(int(_NAME_TO_ID[fund_name]))
        st.session_state.watchlist = [
            _ID_TO_NAME[i] for i in local_state.list_watchlist()
            if i in _ID_TO_NAME
        ]
        return
    wl = st.session_state.watchlist
    if fund_name in wl:
        st.session_state.watchlist = [n for n in wl if n != fund_name]
    elif len(wl) < 10:
        st.session_state.watchlist = wl + [fund_name]


def _push_recent(fund_name: str | None) -> None:
    """Add to the front of recent_funds, dedupe, cap at 5."""
    if not fund_name:
        return
    rf = st.session_state.recent_funds
    if rf and rf[0] == fund_name:
        return
    rf = [fund_name] + [n for n in rf if n != fund_name]
    st.session_state.recent_funds = rf[:5]

# Niet-pensioenfondsen (vermogensbeheerders e.d.) — uitgesloten van
# fonds-selectors en de globale zoeker.
NON_FUNDS = ['APG', 'ASR', 'ASR PPI', 'Allianz', 'Allianz PPI', 'A.S. Watson Nederland']

# Cross-page navigatie: callbacks zetten een pending jump die net vóór
# nav.run() (onderaan dit bestand) wordt afgehandeld met st.switch_page.
# st.switch_page zelf mag niet vanuit een widget-callback worden aangeroepen.
def _queue_fund_jump(fund_name: str) -> None:
    st.session_state._jump_fund = fund_name


def _on_global_search():
    sel = st.session_state.get("global_fund_search")
    if sel:
        _queue_fund_jump(sel)
        st.session_state.global_fund_search = None  # reset voor volgende zoek


# Globale fondszoeker — springt vanaf elke pagina naar de diepteanalyse
st.sidebar.selectbox(
    "Zoek fonds",
    df_funds[~df_funds["name"].isin(NON_FUNDS)]["name"].sort_values().tolist(),
    index=None,
    key="global_fund_search",
    on_change=_on_global_search,
    placeholder="🔎 Spring naar fonds…",
    label_visibility="collapsed",
)

# Dark mode switch — per sessie; default volgt het Streamlit/OS-thema
st.sidebar.toggle("🌙 Donkere modus", key="dark_mode")

st.sidebar.markdown("---")
_dnb_count = 0
_dnb_quarter = "—"
try:
    _row = load_data(
        "SELECT COUNT(DISTINCT fund_id) AS n, "
        "MAX(year)||'Q'||MAX(quarter) AS latest "
        "FROM dnb_quarterly_metrics WHERE metric_name LIKE 'Beleidsdekkingsgraad%'"
    )
    _dnb_count = int(_row['n'][0] or 0)
    _dnb_quarter = str(_row['latest'][0] or '—')
except Exception:
    pass

st.sidebar.markdown(
    f"""
<div class="section-card" style="margin-bottom:0;">
  <div class="section-card-title">Database</div>
  <div style="font-size:12px;color:var(--text-mid);line-height:1.7;">
    <div><strong>{len(df_funds)}</strong> fondsen gevolgd</div>
    <div>Totaal AUM <strong>€{df_funds['aum_euro_bn'].sum():,.1f} Bn</strong></div>
    <div>DNB-dekking <strong>{_dnb_count}</strong> fondsen</div>
    <div>Laatste DNB-kwartaal <strong>{_dnb_quarter}</strong></div>
  </div>
</div>
""",
    unsafe_allow_html=True,
)

# Meldingen (Fase 2) — alerts feed from app_state.db, written by
# scripts/automation/generate_alerts.py after each bi-daily scrape.
if _LOCAL_STATE_OK:
    _n_unread = local_state.unread_alert_count()
    _badge = f" ({_n_unread})" if _n_unread else ""
    with st.sidebar.expander(f"🔔 Meldingen{_badge}", expanded=bool(_n_unread)):
        _alerts = local_state.get_alerts(limit=15)
        if _alerts.empty:
            st.caption("Geen meldingen. Voeg fondsen toe aan je watchlist; de "
                       "scraper genereert daarna meldingen.")
        else:
            _ICONS = {"news": "📰", "jaarverslag": "📑", "dekkingsgraad": "📉"}
            for _, _a in _alerts.iterrows():
                _ic = _ICONS.get(_a["kind"], "•")
                _unread_dot = "🟣 " if not _a["is_read"] else ""
                _fn = _a.get("fund_name") or "—"
                _title = _a["title"]
                if _a["ref_url"]:
                    _title = f"[{_title}]({_a['ref_url']})"
                st.markdown(
                    f"{_unread_dot}{_ic} **{_fn}**<br>"
                    f"<span style='font-size:12px;'>{_title}</span>"
                    f"<br><span style='font-size:10px;color:#8F9BA8;'>"
                    f"{_a['detail'] or ''} · {_a['created_ts'][:10]}</span>",
                    unsafe_allow_html=True,
                )
            if _n_unread and st.button("Markeer alles gelezen",
                                       key="mark_alerts_read",
                                       use_container_width=True):
                local_state.mark_alerts_read()
                st.rerun()

# Recent fondsen — quick jump
if st.session_state.recent_funds:
    st.sidebar.markdown("<br>", unsafe_allow_html=True)

    def _jump_recent(name: str):
        def cb():
            _queue_fund_jump(name)
        return cb

    with st.sidebar.container():
        st.markdown(
            '<div class="section-card-title">Recent bekeken</div>',
            unsafe_allow_html=True,
        )
        for _name in st.session_state.recent_funds:
            st.button(
                _name[:30] + ("…" if len(_name) > 30 else ""),
                key=f"recent_{_name}",
                on_click=_jump_recent(_name),
                use_container_width=True,
            )


# ==========================================
# PAGE 1: SECTOR OVERVIEW
# ==========================================
def page_sector_overview():
    st.title("🇳🇱 Nederlandse pensioenfondsen")
    st.markdown("Interactieve verkenning van de Nederlandse pensioensector — "
                "AUM, dekkingsgraden, allocaties, ESG en de WTP-transitie.")

    valid_aum = df_funds.dropna(subset=['aum_euro_bn'])
    valid_ratio = df_funds.dropna(subset=['dekkingsgraad_pct'])
    largest_row = valid_aum.loc[valid_aum['aum_euro_bn'].idxmax()]
    pct_aum_coverage = len(valid_aum) / len(df_funds) * 100

    # DNB-sectordelta voor het KPI-pijltje (laatste vs vorig kwartaal)
    _bdg = get_dnb_sector_bdg()
    _bdg_delta = None
    _bdg_dir = "up"
    if len(_bdg) == 2:
        _d = float(_bdg['avg_bdg'].iloc[0] - _bdg['avg_bdg'].iloc[1])
        _bdg_delta = f"{_d:+.1f} pp t.o.v. {int(_bdg['year'].iloc[1])}Q{int(_bdg['quarter'].iloc[1])}"
        _bdg_dir = "up" if _d >= 0 else "down"

    render_kpi_row([
        kpi_card("Totaal gevolgd AUM", f"€{valid_aum['aum_euro_bn'].sum():,.1f} Bn",
                 sub=f"over {len(valid_aum)} fondsen ({pct_aum_coverage:.0f}%)"),
        kpi_card("Gem. dekkingsgraad", f"{valid_ratio['dekkingsgraad_pct'].mean():.1f}%",
                 sub=f"{len(valid_ratio)} fondsen",
                 delta=_bdg_delta, delta_dir=_bdg_dir),
        kpi_card("Grootste fonds", str(largest_row['name'])[:24],
                 sub=f"€{largest_row['aum_euro_bn']:,.1f} Bn"),
        kpi_card("Fondsen gevolgd", f"{len(df_funds)}",
                 sub=f"{df_funds['category'].nunique()} categorieën"),
    ])

    # --- Recent WTP news (top 5 from last 30 days) ---
    wtp_recent = load_data("""
        SELECT n.published_date AS date, f.name AS fund, n.title, n.url
        FROM news_articles n JOIN funds f ON n.fund_id = f.id
        WHERE n.title IS NOT NULL AND n.published_date IS NOT NULL
          AND date(n.published_date) >= date('now', '-60 days')
          AND (
                lower(n.title) LIKE '%invar%'
             OR lower(n.title) LIKE '%nieuwe pensioenregeling%'
             OR lower(n.title) LIKE '%transitie%'
             OR lower(n.title) LIKE '%mag over%'
             OR lower(n.title) LIKE '%wtp%'
          )
        ORDER BY date(n.published_date) DESC LIMIT 5
    """)
    if not wtp_recent.empty:
        st.markdown(
            '<div class="section-card-title">🔄 Recente WTP-headlines</div>',
            unsafe_allow_html=True,
        )
        for _, r in wtp_recent.iterrows():
            st.markdown(
                f"- **{r['date']}** · [{r['fund']}] [{r['title']}]({r['url']})"
            )

    # --- Watchlist quick-view ---
    if st.session_state.watchlist:
        watched = df_funds[df_funds['name'].isin(st.session_state.watchlist)].copy()
        if not watched.empty:
            st.markdown(
                '<div class="section-card-title">⭐ Watchlist</div>',
                unsafe_allow_html=True,
            )
            wl_view = watched[['name', 'category', 'aum_euro_bn', 'dekkingsgraad_pct',
                               'beleidsdekkingsgraad_pct']].rename(columns={
                'name': 'Fonds', 'category': 'Categorie',
                'aum_euro_bn': 'AUM (€ Bn)',
                'dekkingsgraad_pct': 'Dekkingsgraad %',
                'beleidsdekkingsgraad_pct': 'Beleidsdg %',
            })
            st.dataframe(
                wl_view, use_container_width=True, hide_index=True,
                column_config={
                    'AUM (€ Bn)': st.column_config.NumberColumn(format="%.1f"),
                    'Dekkingsgraad %': st.column_config.NumberColumn(format="%.1f%%"),
                    'Beleidsdg %': st.column_config.NumberColumn(format="%.1f%%"),
                },
            )

    st.divider()
    
    # Charts Row
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("AUM vs. dekkingsgraad")
        # Scatter Plot
        fig_scatter = px.scatter(
            df_funds.dropna(subset=['aum_euro_bn', 'dekkingsgraad_pct']),
            x="dekkingsgraad_pct", y="aum_euro_bn",
            color="category", color_discrete_map=CATEGORY_COLORS,
            hover_name="name",
            labels={"dekkingsgraad_pct": "Actuele dekkingsgraad (site %)", "aum_euro_bn": "AUM (miljard €)"},
            log_y=True,  # Log-schaal omdat ABP/PFZW de y-as anders volledig domineren
            title="Log(AUM) vs. actuele dekkingsgraad (site)"
        )
        fig_scatter.update_layout(height=420)
        show_chart(fig_scatter)

    with c2:
        st.subheader("Marktaandeel per categorie")
        tree_src = df_funds.dropna(subset=['aum_euro_bn']).copy()
        tree_src['category'] = tree_src['category'].fillna('Onbekend')
        fig_tree = px.treemap(
            tree_src, path=[px.Constant("Sector"), 'category', 'name'],
            values='aum_euro_bn',
            color='category', color_discrete_map={**CATEGORY_COLORS, "(?)": GRID_LINE},
            title="Totaal AUM per categorie → fonds (€ Bn)",
        )
        fig_tree.update_traces(
            hovertemplate="<b>%{label}</b><br>€%{value:,.1f} Bn<extra></extra>",
            marker=dict(cornerradius=3),
        )
        fig_tree.update_layout(height=420, margin=dict(l=4, r=4, t=40, b=4))
        show_chart(fig_tree)

    st.divider()
    head_col, dl_col = st.columns([5, 1])
    with head_col:
        st.subheader("Fondsenoverzicht")
        st.markdown("Selecteer een rij voor een inline preview, of klik in de linkkolom om naar de diepteanalyse te gaan.")

    df_display = df_funds[['name', 'category', 'aum_euro_bn', 'dekkingsgraad_pct', 'equity_allocation_pct', 'uitvoerder']].copy()
    df_display.insert(0, 'Profile Link', df_display['name'].apply(lambda x: f"/fonds?fund={urllib.parse.quote_plus(x)}"))

    # Excel-export van het huidige overzicht (zoals de gebruiker filtert/sorteert)
    with dl_col:
        try:
            _xlsx_buf = io.BytesIO()
            _export_df = df_display.drop(columns=['Profile Link']).rename(columns={
                'name': 'Fonds',
                'category': 'Categorie',
                'aum_euro_bn': 'AUM (€ Bn)',
                'dekkingsgraad_pct': 'Dekkingsgraad %',
                'equity_allocation_pct': 'Aandelen %',
                'uitvoerder': 'Uitvoerder',
            })
            with pd.ExcelWriter(_xlsx_buf, engine='xlsxwriter') as writer:
                _export_df.to_excel(writer, sheet_name='Fondsenoverzicht', index=False)
                ws = writer.sheets['Fondsenoverzicht']
                ws.set_column(0, 0, 36)  # Fondsnaam
                ws.set_column(1, 1, 18)  # Categorie
                ws.set_column(2, 4, 14, writer.book.add_format({'num_format': '#,##0.00'}))
                ws.set_column(5, 5, 28)
                ws.freeze_panes(1, 0)
            st.download_button(
                label="📥 Excel",
                data=_xlsx_buf.getvalue(),
                file_name=f"fondsenoverzicht_{pd.Timestamp.now(tz='UTC').tz_localize(None).date()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        except Exception as e:
            st.caption(f"Excel niet beschikbaar ({e})")

    selection = st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key="fund_directory_table",
        column_config={
            "Profile Link": st.column_config.LinkColumn(
                "Actie",
                help="Klik om de diepteanalyse te openen.",
                display_text="Diepteanalyse →",
            ),
            "name": st.column_config.TextColumn("Fonds"),
            "category": st.column_config.TextColumn("Categorie"),
            "aum_euro_bn": st.column_config.NumberColumn("AUM (€ Bn)", format="%.1f"),
            "dekkingsgraad_pct": st.column_config.NumberColumn("Dekkingsgraad", format="%.1f%%"),
            "equity_allocation_pct": st.column_config.NumberColumn("Aandelen %", format="%.1f%%"),
            "uitvoerder": st.column_config.TextColumn("Uitvoerder"),
        },
    )

    sel_rows = (selection.selection or {}).get("rows") or []
    if sel_rows:
        row = df_display.iloc[sel_rows[0]]
        fund_row = df_funds[df_funds['name'] == row['name']].iloc[0]

        cat = str(fund_row.get('category') or 'Onbekend')
        cat_color = CATEGORY_BADGE.get(cat, "gray")

        sfdr_html = badge("SFDR niet gevonden", "outline")
        if pd.notnull(fund_row.get('sfdr_article')):
            a = str(int(fund_row['sfdr_article']))
            sfdr_html = badge(f"SFDR Art {a}", {"6": "gray", "8": "blue", "9": "green"}.get(a, "purple"))

        st.markdown(
            f"""
<div class="section-card">
  <div class="section-card-title">Preview · {_html.escape(str(row['name']))}</div>
  <div style="margin-bottom:10px;">
    {badge(cat, cat_color)}
    {sfdr_html}
    {badge(f"Uitvoerder: {fund_row.get('uitvoerder') or 'Onbekend'}", "outline")}
  </div>
</div>
""",
            unsafe_allow_html=True,
        )

        render_kpi_row([
            kpi_card("AUM",
                     f"€{fund_row['aum_euro_bn']:,.1f} Bn" if pd.notnull(fund_row['aum_euro_bn']) else "—"),
            kpi_card("Dekkingsgraad (site)",
                     f"{fund_row['dekkingsgraad_pct']:.1f}%" if pd.notnull(fund_row['dekkingsgraad_pct']) else "—"),
            kpi_card("Beleidsdekkingsgraad",
                     f"{fund_row['beleidsdekkingsgraad_pct']:.1f}%" if pd.notnull(fund_row['beleidsdekkingsgraad_pct']) else "—"),
            kpi_card("Aandelenallocatie",
                     f"{fund_row['equity_allocation_pct']:.1f}%" if pd.notnull(fund_row['equity_allocation_pct']) else "—"),
        ])

        b1, b2 = st.columns([1, 5])
        with b1:
            st.button("Open diepteanalyse →", on_click=_queue_fund_jump,
                      args=(str(row['name']),), key="open_dd_from_directory")
        with b2:
            if pd.notnull(fund_row.get('website')) and fund_row['website']:
                st.markdown(f"🌐 [{fund_row['website']}]({fund_row['website']})")

# ==========================================
# PAGE 2: FUND DEEP-DIVE
# ==========================================
def page_fund_deep_dive():
    st.header("Fondsprofiel — diepteanalyse")

    # Fund Selector (Exclude APG as it is an asset manager)
    deep_dive_funds = df_funds[~df_funds['name'].isin(NON_FUNDS)]
    fund_names = deep_dive_funds['name'].sort_values().tolist()

    # Try to initialize the selectbox with the globally selected fund
    default_index = 0
    if st.session_state.selected_fund and st.session_state.selected_fund in fund_names:
        default_index = fund_names.index(st.session_state.selected_fund)

    def update_selected_fund():
        st.session_state.selected_fund = st.session_state.fund_selector_ui

    selected_fund_name = st.selectbox(
        "Zoek een pensioenfonds:",
        fund_names,
        index=default_index,
        key="fund_selector_ui",
        on_change=update_selected_fund
    )

    if not selected_fund_name:
        return

    _push_recent(selected_fund_name)
    fund_data = df_funds[df_funds['name'] == selected_fund_name].iloc[0]
    fund_id = fund_data['id']

    # --- Fondsheader: naam + badges links, acties rechts ---
    cat = str(fund_data.get('category') or 'Onbekend')
    badges_html = badge(cat, CATEGORY_BADGE.get(cat, "gray"))
    if pd.notnull(fund_data.get('sfdr_article')):
        _a = str(int(fund_data['sfdr_article']))
        badges_html += " " + badge(f"SFDR Art {_a}",
                                   {"6": "gray", "8": "blue", "9": "green"}.get(_a, "purple"))
    else:
        badges_html += " " + badge("SFDR onbekend", "outline")
    if pd.notnull(fund_data.get('uitvoerder')) and str(fund_data['uitvoerder']).strip():
        badges_html += " " + badge(f"Uitvoerder: {fund_data['uitvoerder']}", "outline")

    head_l, head_r = st.columns([4, 1.3])
    with head_l:
        st.markdown(
            f'<div class="fund-header">'
            f'<div class="fund-header-name">{_html.escape(str(fund_data["name"]))}</div>'
            f'<div class="fund-header-badges">{badges_html}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        if pd.notnull(fund_data['website']) and fund_data['website'] != "":
            st.markdown(f"🌐 [{fund_data['website']}]({fund_data['website']})")
    with head_r:
        is_watched = fund_data['name'] in st.session_state.watchlist
        st.button(
            "★ Losmaken" if is_watched else "☆ Pin op watchlist",
            key=f"watch_toggle_{fund_id}",
            on_click=_toggle_watch, args=(str(fund_data['name']),),
            use_container_width=True,
            help=("Verwijder van watchlist" if is_watched
                  else "Voeg toe aan watchlist (max 10)"),
        )
        try:
            _hist = get_metrics_history(int(fund_id))
            pdf_bytes = build_fund_factsheet_pdf(fund_data, _hist)
            _safe_name = re.sub(r'[^A-Za-z0-9]+', '_', str(fund_data['name'])).strip('_')
            st.download_button(
                label="📄 PDF Factsheet",
                data=pdf_bytes,
                file_name=f"factsheet_{fund_id}_{_safe_name}.pdf",
                mime="application/pdf",
                use_container_width=True,
            )
        except Exception as e:
            st.caption(f"PDF niet beschikbaar ({e})")

    if 'description' in fund_data and pd.notnull(fund_data['description']) and fund_data['description'] != "":
        st.info(fund_data['description'])

    render_kpi_row([
        kpi_card("AUM",
                 f"€{fund_data['aum_euro_bn']:,.1f} Bn" if pd.notnull(fund_data['aum_euro_bn']) else "—",
                 sub=str(fund_data.get('category') or '')),
        kpi_card("Actuele dekkingsgraad",
                 f"{fund_data['dekkingsgraad_pct']:.1f}%" if pd.notnull(fund_data['dekkingsgraad_pct']) else "—",
                 sub="door fonds zelf gerapporteerd"),
        kpi_card("Aandelenallocatie",
                 f"{fund_data['equity_allocation_pct']:.1f}%" if pd.notnull(fund_data['equity_allocation_pct']) else "—",
                 sub="van totale portefeuille"),
        kpi_card("Deelnemers",
                 f"{fund_data['deelnemers_totaal']:,.0f}".replace(",", ".") if pd.notnull(fund_data['deelnemers_totaal']) else "—",
                 sub="actief + slapers + gepensioneerd"),
    ])

    tab_kern, tab_hist, tab_esg, tab_docs = st.tabs([
        "📊 Kerncijfers", "📈 Historie", "🌱 ESG & duurzaamheid", "📄 Documenten & nieuws",
    ])

    # ---------------- TAB 1: KERNCIJFERS ----------------
    with tab_kern:
        # --- Latest annual report (FY) — surfaces fy_annual_metrics when present ---
        fy_df = get_fund_annual_metrics(fund_id)
        if not fy_df.empty:
            latest_fy = int(fy_df['fiscal_year'].max())
            latest = fy_df[fy_df['fiscal_year'] == latest_fy]
            metrics = {row['metric_name']: row for _, row in latest.iterrows()}
            source_url = next(iter(latest['source_url'].dropna()), None)

            fy_aum = metrics.get('aum_eur_bn', {}).get('value') if 'aum_eur_bn' in metrics else None
            fy_actu = metrics.get('actuele_dekkingsgraad_pct', {}).get('value') if 'actuele_dekkingsgraad_pct' in metrics else None
            fy_beleid = metrics.get('beleidsdekkingsgraad_pct', {}).get('value') if 'beleidsdekkingsgraad_pct' in metrics else None
            fy_eq = metrics.get('equity_allocation_pct', {}).get('value') if 'equity_allocation_pct' in metrics else None

            link_html = (
                f'<a href="{_html.escape(source_url)}" target="_blank" '
                f'style="color:var(--accent);text-decoration:none;font-size:12px;">Source PDF →</a>'
                if source_url else ''
            )
            st.markdown(
                f"""
<div class="section-card" style="margin-top:8px;">
  <div class="section-card-title">Laatste jaarverslag · FY {latest_fy} &nbsp; {link_html}</div>
</div>
""",
                unsafe_allow_html=True,
            )
            render_kpi_row([
                kpi_card("AUM (FY)",
                         f"€{fy_aum:,.1f} Bn" if fy_aum is not None else "—",
                         sub=f"uit jaarverslag {latest_fy}"),
                kpi_card("Actuele dekkingsgraad (FY)",
                         f"{fy_actu:.1f}%" if fy_actu is not None else "—",
                         sub=f"per eind {latest_fy}"),
                kpi_card("Beleidsdekkingsgraad (FY)",
                         f"{fy_beleid:.1f}%" if fy_beleid is not None else "—",
                         sub=f"per eind {latest_fy}"),
                kpi_card("Aandelenallocatie (FY)",
                         f"{fy_eq:.1f}%" if fy_eq is not None else "—",
                         sub=f"per jaarverslag {latest_fy}"),
            ])
            # Markeer waarden die afwijken van de funds-tabel KPI's hierboven
            mismatches = []
            if fy_aum is not None and pd.notnull(fund_data['aum_euro_bn']) and abs(fy_aum - fund_data['aum_euro_bn']) > 0.5:
                mismatches.append(f"AUM (fondsentabel {fund_data['aum_euro_bn']:.1f} vs jaarverslag {fy_aum:.1f})")
            if fy_actu is not None and pd.notnull(fund_data['dekkingsgraad_pct']) and abs(fy_actu - fund_data['dekkingsgraad_pct']) > 1.0:
                mismatches.append(f"dekkingsgraad ({fund_data['dekkingsgraad_pct']:.1f}% vs {fy_actu:.1f}%)")
            if mismatches:
                st.caption("⚠ fondsentabel-waarden wijken af van het jaarverslag: " + "; ".join(mismatches))

        # --- Analyse jaarverslag (from fund_analysis table, LLM-generated) ---
        # Rendered independently of fy_annual_metrics presence — many funds
        # (incl. ABP) have an LLM-generated analysis without parsed numerics.
        analysis = load_data(f"""
            SELECT fiscal_year, summary, highlights_json, lowlights_json,
                   key_risks_json, transitie_status, source_pdf, generated_at
            FROM fund_analysis
            WHERE fund_id = {int(fund_id)}
            ORDER BY fiscal_year DESC LIMIT 1
        """)
        analysis_fy = int(analysis.iloc[0]['fiscal_year']) if not analysis.empty else None
        expander_label = (
            f"📋 Analyse jaarverslag {analysis_fy}" if analysis_fy
            else "📋 Analyse jaarverslag"
        )
        with st.expander(expander_label, expanded=False):
            if analysis.empty:
                st.markdown(
                    "Nog geen analyse beschikbaar. "
                    "Genereer met de lokale LLM-extractor:"
                )
                st.code(
                    f"cd scripts/document_parsing\n"
                    f"python3 llm_extract_analysis.py --funds {int(fund_id)}",
                    language="bash",
                )
            else:
                a = analysis.iloc[0]
                if a['summary']:
                    st.markdown(f"**Samenvatting**  \n{a['summary']}")
                import json as _json
                def _render_bullets(label, raw):
                    if not raw: return
                    try:
                        items = _json.loads(raw)
                    except Exception:
                        return
                    if not items: return
                    st.markdown(f"**{label}**")
                    for b in items:
                        st.markdown(f"- {b}")
                _render_bullets("Highlights", a['highlights_json'])
                _render_bullets("Aandachtspunten", a['lowlights_json'])
                _render_bullets("Belangrijkste risico's", a['key_risks_json'])
                if a['transitie_status']:
                    st.markdown(f"**WTP-transitie**  \n{a['transitie_status']}")
                if a['source_pdf']:
                    st.caption(
                        f"Bron: `{a['source_pdf']}` · gegenereerd {a['generated_at']}"
                    )

        # --- Deelnemers: donut + breakdown ---
        deel_l, deel_r = st.columns([1, 1])
        with deel_l:
            st.markdown("##### Deelnemersopbouw")
            _parts = [
                ("Actief", fund_data.get('deelnemers_actief'), GREEN_FILL),
                ("Slapers", fund_data.get('deelnemers_slapers'), ORANGE_FILL),
                ("Gepensioneerd", fund_data.get('deelnemers_gepensioneerd'), ACCENT),
            ]
            _known = [(lbl, float(v), c) for lbl, v, c in _parts if pd.notnull(v) and v > 0]
            if _known:
                _ddf = pd.DataFrame(_known, columns=['Groep', 'Aantal', 'Kleur'])
                fig_deel = px.pie(
                    _ddf, names='Groep', values='Aantal', hole=0.58,
                    color='Groep',
                    color_discrete_map={lbl: c for lbl, _, c in _known},
                )
                fig_deel.update_traces(
                    textinfo='percent',
                    hovertemplate="<b>%{label}</b><br>%{value:,.0f} deelnemers<extra></extra>",
                )
                _tot = fund_data.get('deelnemers_totaal')
                if pd.notnull(_tot):
                    fig_deel.add_annotation(
                        text=f"<b>{_tot:,.0f}</b><br>totaal".replace(",", "."),
                        showarrow=False, font=dict(size=13),
                    )
                fig_deel.update_layout(height=280, showlegend=True,
                                       margin=dict(l=10, r=10, t=10, b=10))
                show_chart(fig_deel)
            else:
                st.info("Geen deelnemers-uitsplitsing beschikbaar.")
        with deel_r:
            st.markdown("##### Kerngegevens")
            if pd.notnull(fund_data['deelnemers_totaal']):
                st.markdown(f"**Totaal deelnemers:** {fund_data['deelnemers_totaal']:,.0f}")
                st.markdown(f"- **Actief:** {fund_data['deelnemers_actief']:,.0f}" if pd.notnull(fund_data['deelnemers_actief']) else "- **Actief:** n.b.")
                st.markdown(f"- **Slapers:** {fund_data['deelnemers_slapers']:,.0f}" if pd.notnull(fund_data['deelnemers_slapers']) else "- **Slapers:** n.b.")
                st.markdown(f"- **Gepensioneerden:** {fund_data['deelnemers_gepensioneerd']:,.0f}" if pd.notnull(fund_data['deelnemers_gepensioneerd']) else "- **Gepensioneerden:** n.b.")
            st.markdown(f"**Uitvoerder:** {fund_data['uitvoerder'] if pd.notnull(fund_data['uitvoerder']) else 'Onbekend'}")
            if pd.notnull(fund_data['beleidsdekkingsgraad_pct']):
                st.markdown(f"**Beleidsdekkingsgraad:** {fund_data['beleidsdekkingsgraad_pct']:.1f}%")
            _bp_raw = fund_data.get('benchmark_providers')
            if pd.notnull(_bp_raw) and str(_bp_raw).strip():
                _bp_badges = " ".join(
                    badge(p.strip(), PROVIDER_BADGE.get(p.strip(), "outline"))
                    for p in str(_bp_raw).split(", ")
                )
                st.markdown(f"**Benchmark-providers:** {_bp_badges}",
                            unsafe_allow_html=True)
                st.caption("Uit het jaarverslag geëxtraheerd; AEX-treffers "
                           "kunnen ruis zijn.")

    # ---------------- TAB 2: HISTORIE ----------------
    with tab_hist:
        history_df = get_metrics_history(fund_id)
        if not history_df.empty:
            # Numeriek maken om Plotly Express wide-form error te vermijden
            for col in ["beleidsdekkingsgraad_pct", "beleggingsrendement_pct"]:
                if col in history_df.columns:
                    history_df[col] = pd.to_numeric(history_df[col], errors='coerce')

            show_peer = st.toggle(
                "Vergelijk met categorie-gemiddelde",
                value=False,
                help=f"Toon het gemiddelde over alle {fund_data.get('category') or 'overige'} fondsen op dezelfde grafiek.",
            )

            fig_line = px.line(history_df, x="year", y=["beleidsdekkingsgraad_pct", "beleggingsrendement_pct"],
                               labels={"value": "Percentage (%)", "year": "Jaarverslag", "variable": "Metric"},
                               title="Meerjarenoverzicht: dekkingsgraad & rendement (jaarrapportages)")
            fig_line.update_xaxes(dtick=1, tickformat="d")

            if show_peer:
                peer_df = get_peer_history(fund_data.get('category'), int(fund_id))
                if not peer_df.empty:
                    # Add peer averages as dashed lines on the same axis
                    fig_line.add_scatter(
                        x=peer_df['year'], y=peer_df['peer_beleidsdg'],
                        mode='lines+markers', name='peer beleidsdg',
                        line=dict(dash='dot', color=MUTE_LINE),
                    )
                    fig_line.add_scatter(
                        x=peer_df['year'], y=peer_df['peer_rendement'],
                        mode='lines+markers', name='peer rendement',
                        line=dict(dash='dot', color=ORANGE_FILL),
                    )
                    cat_p = fund_data.get('category') or 'Unknown'
                    peer_n = int(peer_df['peer_count'].max()) if not peer_df.empty else 0
                    st.caption(f"Peer-overlay: gemiddelde van **{peer_n}** fondsen in categorie '{cat_p}' (exclusief dit fonds).")
                else:
                    st.caption("Geen peer-data beschikbaar voor deze categorie.")

            show_chart(fig_line)

            # --- Indexatie vs CPI ---
            idx_df = history_df[['year', 'indexatieverlening_pct', 'cpi_pct']].dropna(how='all', subset=['indexatieverlening_pct', 'cpi_pct']).copy()
            if idx_df['indexatieverlening_pct'].notna().any():
                st.markdown("##### Indexatie vs CPI")
                idx_df = idx_df.sort_values('year')
                fig_idx = px.bar(
                    idx_df, x='year', y='indexatieverlening_pct',
                    labels={'indexatieverlening_pct': 'Toeslag (%)', 'year': 'Jaar'},
                )
                fig_idx.update_traces(marker_color=GREEN_FILL, name='Toeslag', showlegend=True)
                # CPI overlay as line
                cpi_line = idx_df.dropna(subset=['cpi_pct'])
                if not cpi_line.empty:
                    fig_idx.add_scatter(
                        x=cpi_line['year'], y=cpi_line['cpi_pct'],
                        mode='lines+markers', name='CPI',
                        line=dict(color=RED_LINE, width=2),
                    )
                fig_idx.update_layout(height=300, showlegend=True)
                show_chart(fig_idx)

                # Cumulative koopkracht: (∏(1+indexatie) / ∏(1+CPI) - 1) ×100
                cum = idx_df.dropna(subset=['indexatieverlening_pct', 'cpi_pct']).sort_values('year').copy()
                if len(cum) >= 2:
                    prod_idx = (1 + cum['indexatieverlening_pct']/100).prod()
                    prod_cpi = (1 + cum['cpi_pct']/100).prod()
                    delta_pct = (prod_idx / prod_cpi - 1) * 100
                    n_yrs = len(cum)
                    label = "koopkracht behouden" if delta_pct >= 0 else "koopkracht verloren"
                    st.caption(
                        f"Cumulatief over **{n_yrs} jaar** ({int(cum['year'].min())}–{int(cum['year'].max())}): "
                        f"**{delta_pct:+.1f}%** {label} t.o.v. CPI."
                    )

            # --- Asset allocation evolutie ---
            aa_df = history_df[['year', 'zakelijke_waarden_pct', 'rente_afdekking_pct']].dropna(
                how='all', subset=['zakelijke_waarden_pct', 'rente_afdekking_pct']
            ).copy()
            if not aa_df.empty:
                st.markdown("##### Beleggingsprofiel & rente-afdekking")
                aa_df = aa_df.sort_values('year')
                fig_aa = px.line(
                    aa_df, x='year',
                    y=[c for c in ['zakelijke_waarden_pct', 'rente_afdekking_pct'] if c in aa_df.columns],
                    markers=True,
                    labels={'value': '%', 'year': 'Jaar', 'variable': 'Metric'},
                )
                fig_aa.update_layout(height=300)
                show_chart(fig_aa)

            st.markdown("#### Meerjarenoverzicht (Jaarrapportages)")

            rename_map = {
                'aum_euro_bn': 'Belegd vermogen (€ mrd)',
                'economische_dekkingsgraad_pct': 'Actuele dekkingsgraad',
                'nominale_dekkingsgraad_pct': 'Nominale dekkingsgraad',
                'beleidsdekkingsgraad_pct': 'Beleidsdekkingsgraad',
                'vereiste_dekkingsgraad_pct': 'Vereiste dekkingsgraad',
                'reele_dekkingsgraad_pct': 'Reële dekkingsgraad',
                'beleggingsrendement_pct': 'Totaal rendement',
                'zakelijke_waarden_pct': '% Zakelijke waarden',
                'rente_afdekking_pct': '% Renteafdekking',
                'rente_afdekking_rendement_pct': 'Rendement renteafdekking',
                'indexatieverlening_pct': 'Indexatie (toeslag)',
                'cpi_pct': 'CPI (Prijsinflatie)',
                'deelnemers_actief': 'Actieve deelnemers',
                'deelnemers_slapers': 'Gewezen deelnemers',
                'deelnemers_pensioengerechtigd': 'Pensioengerechtigden',
                'deelnemers_totaal': 'Totaal deelnemers'
            }

            table_df = history_df.rename(columns=rename_map)

            # Group by year to handle any duplicate database entries for the same year
            table_df = table_df.groupby('year').last().T
            table_df = table_df[sorted(table_df.columns, reverse=True)]
            table_df = table_df.astype(object)  # cellen worden hieronder strings

            for row_name in table_df.index:
                is_pct = any(kw in str(row_name).lower() for kw in ['dekkingsgraad', 'rendement', 'indexatie', 'cpi'])
                for col in table_df.columns:
                    val = table_df.at[row_name, col]
                    if pd.notnull(val):
                        if is_pct:
                            table_df.at[row_name, col] = f"{val:.1f}%"
                        elif 'vermogen' in str(row_name).lower():
                            table_df.at[row_name, col] = f"€{val:,.2f} mrd".replace('.', 'X').replace(',', '.').replace('X', ',')
                        else:
                            table_df.at[row_name, col] = f"{int(float(val)):,}".replace(',', '.')
                    else:
                        table_df.at[row_name, col] = "-"

            table_df.columns = [str(int(c)) for c in table_df.columns]

            styled_table = table_df.style.set_properties(subset=table_df.columns, **{'text-align': 'center'})

            st.dataframe(styled_table, use_container_width=True)
        else:
            st.info("Geen historische metrics beschikbaar voor dit fonds.")

    # ---------------- TAB 3: ESG & DUURZAAMHEID ----------------
    with tab_esg:
        esg_l, esg_r = st.columns([1, 1])
        with esg_l:
            st.markdown("##### SFDR & EU-taxonomie")
            # Toon geëxtraheerde SFDR-metrics indien beschikbaar
            if pd.notnull(fund_data['sfdr_article']):
                article_num = str(int(fund_data['sfdr_article']))
                article_color = {"6": "gray", "8": "blue", "9": "green"}.get(article_num, "purple")
                article_html = badge(f"Artikel {article_num}", article_color)
            else:
                article_html = badge("Niet gevonden", "outline")
            tax_pct = f"{fund_data['eu_taxonomy_pct']}%" if pd.notnull(fund_data['eu_taxonomy_pct']) else "Niet gevonden"

            st.markdown(f"**SFDR-artikel (2024):** {article_html}", unsafe_allow_html=True)
            st.markdown(f"**EU-taxonomie %:** {tax_pct}")
            st.markdown(f"**Gerapporteerde SFDR-classificatie:** {fund_data['sfdr_classification'] if pd.notnull(fund_data['sfdr_classification']) else 'Niet vermeld'}")
            st.markdown(f"**CO₂-doelstelling:** {fund_data['co2_reduction_goal'] if pd.notnull(fund_data['co2_reduction_goal']) else 'Niet vermeld'}")
        with esg_r:
            st.markdown("##### ESG- en duurzaamheidsrapporten")
            esg_reports_df = get_fund_esg_reports(fund_id)
            if not esg_reports_df.empty:
                for _, row in esg_reports_df.iterrows():
                    st.markdown(f"- [{row['title']}]({row['url']})")
            else:
                st.caption("Geen specifieke duurzaamheidsrapporten gevonden.")

        if 'investment_beliefs' in fund_data and pd.notnull(fund_data['investment_beliefs']) and fund_data['investment_beliefs'] != "":
            st.markdown(f"##### Investment Beliefs\n> {fund_data['investment_beliefs']}")

    # ---------------- TAB 4: DOCUMENTEN & NIEUWS ----------------
    with tab_docs:
        doc_l, doc_r = st.columns([1, 1])
        with doc_l:
            st.markdown("##### Historische jaarverslagen")
            reports_df = get_fund_reports(fund_id)
            if not reports_df.empty:
                for _, row in reports_df.iterrows():
                    st.markdown(f"- [{row['title']}]({row['url']})")
            else:
                st.caption("Geen jaarverslagen gevonden in de database.")

            st.markdown("##### Aandelenportefeuille-beheerders")
            managers_df = get_fund_managers(fund_id)
            if not managers_df.empty:
                for mgr in managers_df['manager'].tolist():
                    st.markdown(f"- {mgr}")
            else:
                st.caption("Geen externe beheerders bekend.")
        with doc_r:
            st.markdown("##### Recente nieuwsartikelen")
            news_df = get_fund_news(fund_id)
            if not news_df.empty:
                for _, row in news_df.head(8).iterrows():
                    st.markdown(f"**{row['published_date']}** — [{row['title']}]({row['url']})")
            else:
                st.caption("Geen recente nieuwsartikelen gescraped.")

# ==========================================
# PAGE 2C: FUND COMPARISON (side-by-side, up to 3 funds)
# ==========================================
def page_fund_comparison():
    st.header("Fondsvergelijking")
    st.markdown(
        "Selecteer 2 of 3 fondsen om de kerncijfers en historische ontwikkeling naast elkaar te zien."
    )

    comparable = df_funds[~df_funds['name'].isin(NON_FUNDS)]
    fund_options = comparable['name'].sort_values().tolist()

    selected = st.multiselect(
        "Te vergelijken fondsen (max 3)",
        fund_options,
        default=[],
        max_selections=3,
        placeholder="Begin typen om te zoeken…",
    )

    if len(selected) < 2:
        st.info("Kies minimaal 2 fondsen om de vergelijking te tonen.")
    else:
        # Per-fund snapshot
        snapshot_cols = ['name', 'category', 'aum_euro_bn', 'dekkingsgraad_pct',
                         'beleidsdekkingsgraad_pct', 'equity_allocation_pct',
                         'deelnemers_totaal', 'uitvoerder', 'sfdr_article']
        snap = df_funds[df_funds['name'].isin(selected)][snapshot_cols].set_index('name').reindex(selected)

        # Snapshot card row
        cards = []
        for name in selected:
            row = snap.loc[name]
            cat = str(row.get('category') or 'Onbekend')
            cat_color = CATEGORY_BADGE.get(cat, "gray")
            badge_html = badge(cat, cat_color)
            if pd.notnull(row.get('sfdr_article')):
                a = str(int(row['sfdr_article']))
                badge_html += " " + badge(f"SFDR Art {a}",
                                          {"6": "gray", "8": "blue", "9": "green"}.get(a, "purple"))
            aum = f"€{row['aum_euro_bn']:,.1f} Bn" if pd.notnull(row['aum_euro_bn']) else "—"
            dekk = f"{row['dekkingsgraad_pct']:.1f}%" if pd.notnull(row['dekkingsgraad_pct']) else "—"
            beleids = f"{row['beleidsdekkingsgraad_pct']:.1f}%" if pd.notnull(row['beleidsdekkingsgraad_pct']) else "—"
            deeln = f"{row['deelnemers_totaal']:,.0f}".replace(",", ".") if pd.notnull(row['deelnemers_totaal']) else "—"
            uitv = row.get('uitvoerder') or '—'
            cards.append(
                '<div class="kpi-card">'
                f'<div class="kpi-label">{_html.escape(str(name))}</div>'
                f'<div style="margin-bottom:8px;">{badge_html}</div>'
                f'<div style="font-size:13px;color:var(--text-mid);line-height:1.8;">'
                f'  <strong>AUM</strong> {aum}<br>'
                f'  <strong>Dekkingsgraad</strong> {dekk}<br>'
                f'  <strong>Beleidsdg</strong> {beleids}<br>'
                f'  <strong>Deelnemers</strong> {deeln}<br>'
                f'  <strong>Uitvoerder</strong> {_html.escape(str(uitv))}<br>'
                f'</div></div>'
            )
        render_kpi_row(cards)
        st.divider()

        # Combined history
        st.subheader("Historische beleidsdekkingsgraad")
        ids = [int(df_funds[df_funds['name'] == n]['id'].iloc[0]) for n in selected]
        history_q = f"""
            SELECT f.name AS fund, h.year, h.beleidsdekkingsgraad_pct, h.aum_euro_bn,
                   h.beleggingsrendement_pct, h.deelnemers_totaal
            FROM historical_metrics h JOIN funds f ON h.fund_id = f.id
            WHERE h.fund_id IN ({','.join(map(str, ids))})
            ORDER BY h.year
        """
        hist = load_data(history_q)
        # Dedupe duplicate rows per (fund, year)
        hist = hist.groupby(['fund', 'year'], as_index=False).last()

        if not hist.empty:
            fig_dekk = px.line(
                hist.dropna(subset=['beleidsdekkingsgraad_pct']),
                x='year', y='beleidsdekkingsgraad_pct', color='fund',
                markers=True, labels={'beleidsdekkingsgraad_pct': 'Beleidsdekkingsgraad (%)', 'year': 'Jaar'},
            )
            fig_dekk.add_hline(y=100, line_dash='dot', line_color=RED_LINE,
                               annotation_text='100% minimum', annotation_position='bottom right')
            show_chart(fig_dekk)

            c1, c2 = st.columns(2)
            with c1:
                st.subheader("AUM-ontwikkeling")
                fig_aum = px.line(
                    hist.dropna(subset=['aum_euro_bn']),
                    x='year', y='aum_euro_bn', color='fund', markers=True,
                    labels={'aum_euro_bn': 'AUM (€ Bn)', 'year': 'Jaar'}, log_y=True,
                )
                show_chart(fig_aum)
            with c2:
                st.subheader("Totaal rendement per jaar")
                rend = hist.dropna(subset=['beleggingsrendement_pct'])
                if not rend.empty:
                    fig_rend = px.bar(
                        rend, x='year', y='beleggingsrendement_pct', color='fund',
                        barmode='group',
                        labels={'beleggingsrendement_pct': 'Rendement (%)', 'year': 'Jaar'},
                    )
                    fig_rend.add_hline(y=0, line_color=MUTE_LINE)
                    show_chart(fig_rend)
                else:
                    st.info("Geen rendement-data beschikbaar voor deze selectie.")

            st.subheader("Deelnemers-ontwikkeling")
            dln = hist.dropna(subset=['deelnemers_totaal'])
            if not dln.empty:
                fig_dln = px.line(
                    dln, x='year', y='deelnemers_totaal', color='fund', markers=True,
                    labels={'deelnemers_totaal': 'Totaal aantal deelnemers', 'year': 'Jaar'},
                    log_y=True,
                )
                show_chart(fig_dln)
            else:
                st.info("Geen deelnemers-tijdreeks beschikbaar voor deze selectie.")
        else:
            st.info("Geen historische data voor de geselecteerde fondsen.")


# ==========================================
# PAGE 2D: TRENDS — biggest movers
# ==========================================
def page_trends():
    st.header("Trends — Grootste mutaties")
    st.markdown(
        "Fondsen met de grootste verandering in beleidsdekkingsgraad, AUM "
        "en rendement op DNB-data. Kies een tijdvenster en zie wie het "
        "afgelopen jaar het sterkst is bewogen."
    )

    _window_labels = {1: "1 kwartaal", 4: "1 jaar (4Q)", 8: "2 jaar", 12: "3 jaar"}
    if hasattr(st, "segmented_control"):
        window = st.segmented_control(
            "Tijdvenster", options=[1, 4, 8, 12],
            format_func=lambda q: _window_labels[q], default=4,
        ) or 4
    else:  # fallback voor oudere Streamlit (Python 3.9-deploy)
        window = st.radio(
            "Tijdvenster", options=[1, 4, 8, 12],
            format_func=lambda q: _window_labels[q], index=1, horizontal=True,
        )

    # Latest period in DNB data
    latest = load_data("""
        SELECT MAX(year * 10 + quarter) AS key, year, quarter
        FROM dnb_quarterly_metrics
        WHERE metric_name='Beleidsdekkingsgraad' AND value IS NOT NULL
    """)
    if latest.empty or latest['key'].iloc[0] is None:
        st.info("Geen DNB-data beschikbaar.")
    else:
        cur_y = int(latest['year'].iloc[0])
        cur_q = int(latest['quarter'].iloc[0])
        # earlier period
        ago_quarters = cur_q - window
        prev_y = cur_y
        while ago_quarters <= 0:
            prev_y -= 1
            ago_quarters += 4
        prev_q = ago_quarters

        st.caption(f"Huidig: **{cur_y}Q{cur_q}** vergeleken met **{prev_y}Q{prev_q}**")

        def movers(metric: str, label: str, unit_format: str = "{:+.1f} pp"):
            df = load_data(f"""
                WITH now AS (
                    SELECT fund_id, value FROM dnb_quarterly_metrics
                    WHERE metric_name = '{metric}' AND year = {cur_y} AND quarter = {cur_q}
                      AND value IS NOT NULL
                ),
                then_ AS (
                    SELECT fund_id, value FROM dnb_quarterly_metrics
                    WHERE metric_name = '{metric}' AND year = {prev_y} AND quarter = {prev_q}
                      AND value IS NOT NULL
                )
                SELECT f.id AS fund_id, f.name AS fund, f.category,
                       n.value AS now_val, t.value AS then_val,
                       (n.value - t.value) AS delta
                FROM now n JOIN then_ t ON t.fund_id = n.fund_id
                JOIN funds f ON f.id = n.fund_id
                ORDER BY delta DESC
            """)
            if df.empty:
                st.info(f"Geen data voor {label}.")
                return
            top = df.head(10).copy()
            bot = df.tail(10).copy().sort_values('delta')
            st.markdown(f"### {label}")
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Top 10 stijgers**")
                top_show = top[['fund', 'category', 'now_val', 'then_val', 'delta']].rename(
                    columns={'fund': 'Fonds', 'category': 'Categorie',
                             'now_val': f'{cur_y}Q{cur_q}', 'then_val': f'{prev_y}Q{prev_q}',
                             'delta': 'Δ'}
                )
                st.dataframe(top_show, use_container_width=True, hide_index=True,
                             column_config={
                                 f'{cur_y}Q{cur_q}': st.column_config.NumberColumn(format="%.1f"),
                                 f'{prev_y}Q{prev_q}': st.column_config.NumberColumn(format="%.1f"),
                                 'Δ': st.column_config.NumberColumn(format=unit_format.replace("{:", "").replace("}", "")),
                             })
            with c2:
                st.markdown("**Top 10 dalers**")
                bot_show = bot[['fund', 'category', 'now_val', 'then_val', 'delta']].rename(
                    columns={'fund': 'Fonds', 'category': 'Categorie',
                             'now_val': f'{cur_y}Q{cur_q}', 'then_val': f'{prev_y}Q{prev_q}',
                             'delta': 'Δ'}
                )
                st.dataframe(bot_show, use_container_width=True, hide_index=True,
                             column_config={
                                 f'{cur_y}Q{cur_q}': st.column_config.NumberColumn(format="%.1f"),
                                 f'{prev_y}Q{prev_q}': st.column_config.NumberColumn(format="%.1f"),
                                 'Δ': st.column_config.NumberColumn(format=unit_format.replace("{:", "").replace("}", "")),
                             })

        movers('Beleidsdekkingsgraad', 'Beleidsdekkingsgraad (Δ in pp)')
        st.divider()

        # AUM in mln EUR (DNB unit) — convert delta to Bn
        df_aum = load_data(f"""
            WITH now AS (
                SELECT fund_id, value/1e6 AS bn FROM dnb_quarterly_metrics
                WHERE metric_name = 'Beleggingen voor risico fonds' AND year={cur_y} AND quarter={cur_q}
                  AND value IS NOT NULL
            ),
            then_ AS (
                SELECT fund_id, value/1e6 AS bn FROM dnb_quarterly_metrics
                WHERE metric_name = 'Beleggingen voor risico fonds' AND year={prev_y} AND quarter={prev_q}
                  AND value IS NOT NULL
            )
            SELECT f.name AS fund, f.category,
                   ROUND(n.bn, 2) AS now_bn, ROUND(t.bn, 2) AS then_bn,
                   ROUND(n.bn - t.bn, 2) AS delta_bn,
                   ROUND((n.bn - t.bn)/t.bn*100, 1) AS pct
            FROM now n JOIN then_ t ON t.fund_id = n.fund_id
            JOIN funds f ON f.id = n.fund_id
            WHERE t.bn > 0
            ORDER BY pct DESC
        """)
        if not df_aum.empty:
            st.markdown("### AUM (Δ relatief)")
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Top 10 AUM-groeiers (%)**")
                st.dataframe(df_aum.head(10).rename(columns={
                    'fund': 'Fonds', 'category': 'Categorie',
                    'now_bn': f'{cur_y}Q{cur_q} (€ Bn)', 'then_bn': f'{prev_y}Q{prev_q} (€ Bn)',
                    'delta_bn': 'Δ Bn', 'pct': 'Δ %'
                }), use_container_width=True, hide_index=True)
            with c2:
                st.markdown("**Top 10 AUM-krimpers (%)**")
                st.dataframe(df_aum.tail(10).sort_values('pct').rename(columns={
                    'fund': 'Fonds', 'category': 'Categorie',
                    'now_bn': f'{cur_y}Q{cur_q} (€ Bn)', 'then_bn': f'{prev_y}Q{prev_q} (€ Bn)',
                    'delta_bn': 'Δ Bn', 'pct': 'Δ %'
                }), use_container_width=True, hide_index=True)


# ==========================================
# PAGE 2B: EQUITY STRATEGY DEEP-DIVE
# ==========================================
def page_equity_strategy():
    st.header("📈 Aandelenstrategie: mid-market (1–5 Bn)")
    st.markdown("Overzicht van de aandelenbeleggingen, strategie-notities en externe beheerders voor middelgrote pensioenfondsen (1 tot 5 miljard AUM).")

    query_eq = """
    SELECT
        f.name as "Pensioenfonds",
        f.aum_euro_bn as "AUM (€ Bn)",
        f.equity_allocation_pct as "Aandelen %",
        f.equity_beheerkosten_pct as "Aandelen Beheerkosten %",
        f.equity_transactiekosten_pct as "Aandelen Transactie %",
        f.equity_performance_fee_mln as "Aandelen Perf. Fee (€ Mln)",
        f.equity_strategy_notes as "Strategie-notities",
        GROUP_CONCAT(e.fund_name, ', ') as "Externe beheerders"
    FROM funds f
    LEFT JOIN equity_portfolio_funds e ON f.id = e.fund_id
    WHERE f.aum_euro_bn >= 1.0 AND f.aum_euro_bn <= 5.0
    GROUP BY f.id
    ORDER BY f.aum_euro_bn DESC
    """
    eq_df = load_data(query_eq)

    if not eq_df.empty:
        # KPI-tegels voor het cohort
        valid_eq = eq_df.dropna(subset=['Aandelen %'])
        valid_kosten = eq_df.dropna(subset=['Aandelen Beheerkosten %'])
        with_notes = eq_df['Strategie-notities'].notna() & (eq_df['Strategie-notities'].astype(str).str.strip() != '')
        with_managers = eq_df['Externe beheerders'].notna() & (eq_df['Externe beheerders'].astype(str).str.strip() != '')

        render_kpi_row([
            kpi_card("Fondsen in cohort", f"{len(eq_df)}",
                     sub="€1–5 Bn AUM-range"),
            kpi_card("Totaal AUM in cohort",
                     f"€{eq_df['AUM (€ Bn)'].sum():,.1f} Bn",
                     sub=f"gem. €{eq_df['AUM (€ Bn)'].mean():,.1f} Bn / fonds"),
            kpi_card("Gem. aandelenallocatie",
                     f"{valid_eq['Aandelen %'].mean():.1f}%" if not valid_eq.empty else "—",
                     sub=f"{len(valid_eq)}/{len(eq_df)} rapporteren"),
            kpi_card("Gem. beheerkosten",
                     f"{valid_kosten['Aandelen Beheerkosten %'].mean():.3f}%" if not valid_kosten.empty else "—",
                     sub=f"{len(valid_kosten)}/{len(eq_df)} rapporteren"),
        ])
        st.markdown(
            " ".join([
                badge(f"Met strategie-notities: {int(with_notes.sum())}", "blue"),
                badge(f"Met externe beheerders: {int(with_managers.sum())}", "purple"),
                badge(f"Ontbrekende data: {len(eq_df) - int((with_notes | with_managers).sum())}", "outline"),
            ]),
            unsafe_allow_html=True,
        )
        st.divider()

        st.dataframe(
            eq_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "AUM (€ Bn)": st.column_config.NumberColumn("AUM (€ Bn)", format="%.2f"),
                "Aandelen %": st.column_config.NumberColumn("Aandelen %", format="%.1f%%"),
                "Aandelen Beheerkosten %": st.column_config.NumberColumn("Beheerkosten", format="%.3f%%"),
                "Aandelen Transactie %": st.column_config.NumberColumn("Transactiekosten", format="%.3f%%"),
                "Aandelen Perf. Fee (€ Mln)": st.column_config.NumberColumn("Perf. fee (€ mln)", format="%.2f"),
                "Strategie-notities": st.column_config.TextColumn("Strategie-notities", width="large"),
                "Externe beheerders": st.column_config.TextColumn("Externe beheerders", width="medium"),
            },
        )
    else:
        st.info("Geen fondsen gevonden in de 1–5 Bn AUM-range.")

# ==========================================
# PAGE 3: ASSET MANAGERS EXPOSURE
# ==========================================
def page_asset_managers():
    st.header("Externe vermogensbeheerders (aandelenportefeuilles)")
    st.markdown("Welke externe beleggingshuizen de aandelenportefeuilles van Nederlandse pensioenfondsen beheren.")

    query = """
    SELECT e.fund_name as Beheerder, COUNT(e.fund_id) as Aantal_pensioenfondsen
    FROM equity_portfolio_funds e
    GROUP BY e.fund_name
    ORDER BY Aantal_pensioenfondsen DESC
    LIMIT 20
    """
    managers_df = load_data(query)
    total_managers = load_data("SELECT COUNT(DISTINCT fund_name) AS n FROM equity_portfolio_funds")['n'][0]
    total_relations = load_data("SELECT COUNT(*) AS n FROM equity_portfolio_funds")['n'][0]
    top_mgr = managers_df.iloc[0] if not managers_df.empty else None

    render_kpi_row([
        kpi_card("Beheerders gevolgd", f"{total_managers:,}".replace(',', '.'),
                 sub="unieke beheerder-namen"),
        kpi_card("Totaal relaties", f"{total_relations:,}".replace(',', '.'),
                 sub="beheerder × fonds-mandaten"),
        kpi_card("Meest ingezette beheerder", str(top_mgr['Beheerder'])[:24] if top_mgr is not None else "—",
                 sub=f"{int(top_mgr['Aantal_pensioenfondsen'])} pensioenfondsen" if top_mgr is not None else ""),
        kpi_card("Top-20 dekking",
                 f"{managers_df['Aantal_pensioenfondsen'].sum() / max(total_relations,1) * 100:.0f}%" if total_relations else "—",
                 sub="aandeel van alle relaties in top 20"),
    ])
    st.divider()

    fig_bar = px.bar(managers_df.sort_values("Aantal_pensioenfondsen"),
                     y="Beheerder", x="Aantal_pensioenfondsen", orientation="h",
                     title="Top 20 vermogensbeheerders naar aantal Nederlandse pensioenfonds-cliënten",
                     labels={"Aantal_pensioenfondsen": "Aantal fondsen"})
    fig_bar.update_traces(marker_color=ACCENT,
                          hovertemplate="<b>%{y}</b><br>%{x} fondsen<extra></extra>")
    fig_bar.update_layout(height=560)
    show_chart(fig_bar)

    st.dataframe(managers_df, use_container_width=True)

    st.caption("Benchmark-providers per fonds staan op de aparte pagina "
               "**Benchmarks** (groep Analyse).")

# ==========================================
# PAGE 3B: BENCHMARKS PER FONDS
# ==========================================
def page_benchmarks():
    st.header("Benchmarks per pensioenfonds")
    st.markdown(
        "Tegen welke indices en indexproviders de fondsen hun portefeuille "
        "afmeten. Geëxtraheerd uit jaarverslagen en ABTN's; voor APF-kringen "
        "zonder eigen verslag geërfd van zusterkringen of het moederfonds "
        "(herkenbaar aan de bron-kolom)."
    )

    bp_df = df_funds.dropna(subset=['benchmark_providers']).copy()
    bp_df = bp_df[bp_df['benchmark_providers'].astype(str).str.strip() != '']
    if bp_df.empty:
        st.info("Nog geen benchmark-providers afgeleid. Draai "
                "`db_management/derive_benchmark_providers.py --apply`.")
        return

    bp_long = (
        bp_df.assign(provider=bp_df['benchmark_providers'].str.split(', '))
             .explode('provider')
    )
    top = bp_long.groupby('provider').size().sort_values(ascending=False)
    n_inherited = int(bp_df['benchmark_providers_bron'].notna().sum())

    render_kpi_row([
        kpi_card("Fondsen met benchmark-data", f"{len(bp_df)}",
                 sub=f"van {len(df_funds)} ({len(bp_df)/max(len(df_funds),1)*100:.0f}%)"),
        kpi_card("Meest gebruikt", str(top.index[0]),
                 sub=f"{int(top.iloc[0])} fondsen"),
        kpi_card("Unieke providers", f"{bp_long['provider'].nunique()}",
                 sub="canonieke namen"),
        kpi_card("Geërfd (APF-kringen)", f"{n_inherited}",
                 sub="providerset van zusterkring/moeder"),
    ])
    st.divider()

    # --- Totaaloverzicht ---
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Fondsen per provider")
        bp_counts = top.reset_index()
        bp_counts.columns = ['provider', 'Aantal fondsen']
        fig_bp = px.bar(
            bp_counts.sort_values('Aantal fondsen'),
            y='provider', x='Aantal fondsen', orientation='h',
            labels={'provider': ''},
        )
        fig_bp.update_traces(marker_color=BLUE_FILL,
                             hovertemplate="<b>%{y}</b><br>%{x} fondsen<extra></extra>")
        fig_bp.update_layout(height=420)
        show_chart(fig_bp)
    with c2:
        st.subheader("Provider × categorie")
        ct = pd.crosstab(bp_long['provider'],
                         bp_long['category'].fillna('Onbekend'))
        ct = ct.loc[top.index]  # zelfde volgorde als de bar
        fig_hm = px.imshow(
            ct, text_auto=True, aspect='auto',
            color_continuous_scale=[[0, CHART_BG], [1, ACCENT]],
            labels=dict(x='Categorie', y='', color='Fondsen'),
        )
        fig_hm.update_coloraxes(showscale=False)
        fig_hm.update_layout(height=420)
        show_chart(fig_hm)

    st.divider()

    # --- Per fonds ---
    st.subheader("Per fonds")
    f1, f2, f3 = st.columns([2, 2, 2])
    with f1:
        prov_filter = st.multiselect(
            "Provider", sorted(bp_long['provider'].unique().tolist()),
            placeholder="alle providers")
    with f2:
        cat_filter = st.multiselect(
            "Categorie", sorted(bp_df['category'].dropna().unique().tolist()),
            placeholder="alle categorieën")
    with f3:
        kw = st.text_input("Zoek in fonds of benchmarknaam",
                           placeholder="bv. MSCI World, ABP")
    show_missing = st.toggle("Toon ook fondsen zonder benchmark-data", value=False)

    view = (df_funds.copy() if show_missing else bp_df.copy())
    view['benchmark_providers'] = view['benchmark_providers'].fillna('—')
    raw = view['benchmarks'].astype(str)
    view['benchmarks_raw'] = raw.where(
        view['benchmarks'].notna()
        & ~raw.str.contains('None explicitly', na=False), '—')
    view['bron'] = view['benchmark_providers_bron'].fillna('jaarverslag-extractie')
    view.loc[view['benchmark_providers'] == '—', 'bron'] = '—'

    if prov_filter:
        view = view[view['benchmark_providers'].apply(
            lambda s: any(p in str(s) for p in prov_filter))]
    if cat_filter:
        view = view[view['category'].isin(cat_filter)]
    if kw:
        mask = (view['name'].str.contains(kw, case=False, na=False)
                | view['benchmarks_raw'].str.contains(kw, case=False, na=False)
                | view['benchmark_providers'].str.contains(kw, case=False, na=False))
        view = view[mask]

    st.caption(f"**{len(view)}** fondsen getoond")

    tbl = view[['name', 'category', 'aum_euro_bn', 'benchmark_providers',
                'bron', 'benchmarks_raw']].sort_values('name').copy()
    tbl.insert(0, 'link', view.sort_values('name')['name'].apply(
        lambda x: f"/fonds?fund={urllib.parse.quote_plus(str(x))}"))

    dl_col, _ = st.columns([1, 5])
    with dl_col:
        try:
            _buf = io.BytesIO()
            _exp = tbl.drop(columns=['link']).rename(columns={
                'name': 'Fonds', 'category': 'Categorie',
                'aum_euro_bn': 'AUM (€ Bn)',
                'benchmark_providers': 'Benchmark-providers',
                'bron': 'Bron', 'benchmarks_raw': 'Benchmarks (ruw)'})
            with pd.ExcelWriter(_buf, engine='xlsxwriter') as writer:
                _exp.to_excel(writer, sheet_name='Benchmarks', index=False)
                ws = writer.sheets['Benchmarks']
                ws.set_column(0, 0, 36); ws.set_column(1, 2, 14)
                ws.set_column(3, 3, 50); ws.set_column(4, 4, 32)
                ws.set_column(5, 5, 70); ws.freeze_panes(1, 0)
            st.download_button(
                "📥 Excel", data=_buf.getvalue(),
                file_name=f"benchmarks_{pd.Timestamp.now(tz='UTC').tz_localize(None).date()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True)
        except Exception as e:
            st.caption(f"Excel niet beschikbaar ({e})")

    st.dataframe(
        tbl, use_container_width=True, hide_index=True,
        column_config={
            'link': st.column_config.LinkColumn(
                "Actie", display_text="Diepteanalyse →",
                help="Open de fonds-diepteanalyse."),
            'name': st.column_config.TextColumn("Fonds"),
            'category': st.column_config.TextColumn("Categorie"),
            'aum_euro_bn': st.column_config.NumberColumn("AUM (€ Bn)", format="%.1f"),
            'benchmark_providers': st.column_config.TextColumn("Benchmark-providers", width="medium"),
            'bron': st.column_config.TextColumn("Bron", width="small"),
            'benchmarks_raw': st.column_config.TextColumn("Benchmarks (ruw, uit jaarverslag)", width="large"),
        },
    )
    st.caption(
        "⚠ Ruwe benchmarknamen zijn regex-extracties en kunnen afgekapt zijn; "
        "AEX-treffers kunnen ruis zijn. Geërfde kringen-waarden zijn een "
        "aanname op basis van het APF-platform, geen eigen verslaglegging."
    )


# ==========================================
# PAGE 4: WTP TRACKER
# ==========================================
def page_wtp_tracker():
    st.header("WTP-transitietracker")
    st.markdown("Overzicht van geplande transitiedata naar het nieuwe pensioenstelsel (Wet Toekomst Pensioenen).")

    # After normalize_wtp_fields.py: wtp_invaren ∈ {'ja','nee','uitgesteld',NULL},
    # wtp_transitie_datum is ISO YYYY-MM-DD or NULL. The "WTP plan known"
    # criterion is now "has either a planned date or an invaren status".
    query = """
    SELECT name, aum_euro_bn, wtp_transitie_datum, wtp_contract_type,
           wtp_invaren, wtp_oorspr_datum, uitvoerder
    FROM funds
    WHERE wtp_transitie_datum IS NOT NULL OR wtp_invaren IS NOT NULL
    """
    wtp_df = load_data(query)
    total_funds_in_db = len(df_funds)
    wtp_known = len(wtp_df)
    today_iso = pd.Timestamp.now(tz='UTC').tz_localize(None).date().isoformat()
    if not wtp_df.empty:
        contract_upper = wtp_df['wtp_contract_type'].astype(str).str.lower()
        solidair_count = int(contract_upper.str.startswith('solidair').sum()
                             + contract_upper.eq('spr').sum())
        flexibel_count = int(contract_upper.str.startswith('flex').sum()
                             + contract_upper.eq('fpr').sum())
        past_mask = (wtp_df['wtp_transitie_datum'].notna()
                     & (wtp_df['wtp_transitie_datum'] <= today_iso))
        future_mask = (wtp_df['wtp_transitie_datum'].notna()
                       & (wtp_df['wtp_transitie_datum'] > today_iso))
        reeds_ingevaren = int(past_mask.sum())
        nog_te_komen = int(future_mask.sum())
        uitgesteld_count = int((
            future_mask
            & wtp_df['wtp_oorspr_datum'].notna()
            & (wtp_df['wtp_oorspr_datum'] < wtp_df['wtp_transitie_datum'])
        ).sum())
    else:
        solidair_count = flexibel_count = reeds_ingevaren = nog_te_komen = uitgesteld_count = 0

    render_kpi_row([
        kpi_card("WTP-plan bekend", f"{wtp_known}",
                 sub=f"van {total_funds_in_db} gevolgde fondsen ({wtp_known/max(total_funds_in_db,1)*100:.0f}%)"),
        kpi_card("Reeds ingevaren", f"{reeds_ingevaren}",
                 sub=f"{nog_te_komen} nog te komen · {uitgesteld_count} uitgesteld"),
        kpi_card("Solidair", f"{solidair_count}",
                 sub=f"Flexibel: {flexibel_count}"),
        kpi_card("Gem. AUM in scope",
                 f"€{wtp_df['aum_euro_bn'].mean():,.1f} Bn" if not wtp_df.empty and wtp_df['aum_euro_bn'].notna().any() else "—",
                 sub="onder fondsen met WTP-plan"),
    ])
    st.divider()

    # --- Recente WTP-updates (news headlines mentioning invaren/transitie) ---
    st.markdown(
        '<div class="section-card-title">🔄 Recente WTP-updates uit het nieuws</div>',
        unsafe_allow_html=True,
    )
    wtp_news = load_data("""
        SELECT n.published_date AS "Date",
               f.name AS "Fund",
               n.title AS "Headline",
               n.url,
               f.category AS "Category"
        FROM news_articles n JOIN funds f ON n.fund_id = f.id
        WHERE n.title IS NOT NULL AND n.published_date IS NOT NULL
          AND date(n.published_date) >= date('now', '-180 days')
          AND (
                lower(n.title) LIKE '%invar%'
             OR lower(n.title) LIKE '%nieuwe pensioenregeling%'
             OR lower(n.title) LIKE '%nieuw pensioenstelsel%'
             OR lower(n.title) LIKE '%nieuwe regels%pensioen%'
             OR lower(n.title) LIKE '%nieuwe regels voor%'
             OR lower(n.title) LIKE '%transitie%'
             OR lower(n.title) LIKE '%overstap%pensioen%'
             OR lower(n.title) LIKE '%mag over%'
             OR lower(n.title) LIKE '%wtp%'
          )
        ORDER BY date(n.published_date) DESC, n.id DESC
        LIMIT 60
    """)

    if wtp_news.empty:
        st.info("Geen WTP-gerelateerde nieuwsartikelen gevonden in de laatste 180 dagen.")
    else:
        # Tag each headline with a signal badge
        STRONG = ("mag over", "is ingevaren", "ingevaren", "stemt voor", "gaat de nieuwe", "gaat over op", "overgegaan")
        PLAN   = ("implementatieplan", "transitieplan", "stap op weg", "besluit", "klaar", "overstap")
        def tag(title: str) -> str:
            t = (title or "").lower()
            if any(s in t for s in STRONG):
                return badge("✓ Goedgekeurd / ingevaren", "green")
            if any(s in t for s in PLAN):
                return badge("Planning / voorbereiding", "blue")
            return badge("Communicatie", "outline")

        wtp_news = wtp_news.copy()
        wtp_news["Signal"] = wtp_news["Headline"].apply(tag)

        # Filters
        f1, f2 = st.columns([2, 3])
        with f1:
            fund_filter = st.multiselect(
                "Filter op fonds",
                sorted(wtp_news["Fund"].unique().tolist()),
                placeholder="alle fondsen",
            )
        with f2:
            kw = st.text_input("Zoek in titels", placeholder="bv. ingevaren, beton, implementatieplan")

        view = wtp_news.copy()
        if fund_filter:
            view = view[view["Fund"].isin(fund_filter)]
        if kw:
            view = view[view["Headline"].str.contains(kw, case=False, na=False)]

        st.caption(
            f"**{len(view)}** van **{len(wtp_news)}** WTP-updates getoond "
            f"(periode {wtp_news['Date'].min()} → {wtp_news['Date'].max()})"
        )

        # Counts per signal in current filter
        sig_strong = view["Headline"].apply(lambda t: any(s in (t or '').lower() for s in STRONG)).sum()
        sig_plan = view["Headline"].apply(lambda t: any(s in (t or '').lower() for s in PLAN)).sum()
        st.markdown(
            " ".join([
                badge(f"✓ Goedgekeurd / ingevaren: {int(sig_strong)}", "green"),
                badge(f"Planning / voorbereiding: {int(sig_plan)}", "blue"),
                badge(f"Overige communicatie: {len(view) - int(sig_strong) - int(sig_plan)}", "outline"),
            ]),
            unsafe_allow_html=True,
        )

        st.dataframe(
            view[["Date", "Fund", "Headline", "url", "Category"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "Date": st.column_config.TextColumn("Datum"),
                "Fund": st.column_config.TextColumn("Fonds"),
                "Headline": st.column_config.TextColumn("Kop", width="large"),
                "url": st.column_config.LinkColumn("Bron", display_text="Open →"),
                "Category": st.column_config.TextColumn("Categorie"),
            },
        )

        # Per-fund counts: which funds had the most WTP coverage recently
        per_fund = (
            view.groupby("Fund")
                .size().reset_index(name="updates")
                .sort_values("updates", ascending=False).head(12)
        )
        if len(per_fund) > 1:
            with st.expander("Top 12 fondsen op aantal WTP-updates in de selectie", expanded=False):
                st.dataframe(per_fund, use_container_width=True, hide_index=True)

    st.divider()
    
    if not wtp_df.empty:
        # Split into past (transitie voltooid), future (gepland) en geen-transitie.
        # wtp_invaren='ja' is in de PensioenPro-bron een *intentie*, niet een
        # voltooiingssignaal — classificeren dus puur op datum.
        today_iso = pd.Timestamp.now(tz='UTC').tz_localize(None).date().isoformat()
        wtp_df = wtp_df.copy()
        wtp_df['sort_date'] = wtp_df['wtp_transitie_datum'].fillna('2099-12-31')
        wtp_df['is_past'] = (
            wtp_df['wtp_transitie_datum'].notna()
            & (wtp_df['wtp_transitie_datum'] <= today_iso)
        )
        wtp_df['is_future'] = (
            wtp_df['wtp_transitie_datum'].notna()
            & (wtp_df['wtp_transitie_datum'] > today_iso)
        )

        past_df = wtp_df[wtp_df['is_past']].drop(columns=['is_past', 'is_future'])
        future_df = wtp_df[wtp_df['is_future']].drop(columns=['is_past', 'is_future'])
        no_transition_df = wtp_df[~wtp_df['is_past'] & ~wtp_df['is_future']].drop(columns=['is_past', 'is_future'])

        c1, c2 = st.columns(2)
        with c1:
            timeline_src = future_df.dropna(subset=['wtp_transitie_datum']).copy()
            if not timeline_src.empty:
                timeline_src['Status'] = 'Origineel gepland'
                mask_uitg = (
                    timeline_src['wtp_oorspr_datum'].notna()
                    & (timeline_src['wtp_oorspr_datum'] < timeline_src['wtp_transitie_datum'])
                )
                timeline_src.loc[mask_uitg, 'Status'] = 'Uitgesteld'
                timeline = (
                    timeline_src.groupby(['wtp_transitie_datum', 'Status']).size()
                                .reset_index(name='Aantal fondsen')
                                .rename(columns={'wtp_transitie_datum': 'Transitiedatum'})
                                .sort_values('Transitiedatum')
                )
                fig_bar = px.bar(
                    timeline, x='Transitiedatum', y='Aantal fondsen', color='Status',
                    title="Geplande transities per datum",
                    color_discrete_map={'Origineel gepland': ACCENT, 'Uitgesteld': ORANGE_FILL},
                )
                fig_bar.update_xaxes(type='category')
                show_chart(fig_bar)
            else:
                st.info("Geen geplande transities meer — alles is voltooid of zonder datum.")

        with c2:
            contract_counts = (
                wtp_df['wtp_contract_type']
                    .dropna().loc[lambda s: s.str.strip() != '']
                    .value_counts().reset_index()
            )
            contract_counts.columns = ['Contracttype', 'Aantal']
            fig_ct = px.bar(contract_counts.sort_values('Aantal'),
                            y='Contracttype', x='Aantal', orientation='h',
                            title="Contracttypes")
            fig_ct.update_traces(marker_color=ACCENT,
                                 hovertemplate="<b>%{y}</b><br>%{x} fondsen<extra></extra>")
            fig_ct.update_layout(height=340)
            show_chart(fig_ct)

        # --- Uitgestelde transities (bron: PensioenPro 27-4-2026 PDF) ---
        if 'wtp_oorspr_datum' in wtp_df.columns:
            uitstel_df = wtp_df.dropna(subset=['wtp_oorspr_datum', 'wtp_transitie_datum']).copy()
            uitstel_df = uitstel_df[uitstel_df['wtp_oorspr_datum'] != uitstel_df['wtp_transitie_datum']]
            if not uitstel_df.empty:
                uitstel_df['oud'] = pd.to_datetime(uitstel_df['wtp_oorspr_datum'], errors='coerce')
                uitstel_df['nieuw'] = pd.to_datetime(uitstel_df['wtp_transitie_datum'], errors='coerce')
                uitstel_df['Verschuiving (mnd)'] = (
                    (uitstel_df['nieuw'].dt.year - uitstel_df['oud'].dt.year) * 12
                    + (uitstel_df['nieuw'].dt.month - uitstel_df['oud'].dt.month)
                )
                vooruit = (uitstel_df['Verschuiving (mnd)'] > 0).sum()
                achteruit = (uitstel_df['Verschuiving (mnd)'] < 0).sum()

                st.markdown(f"### Fondsen met uitgestelde transitie ({vooruit} verschoven, {achteruit} vervroegd)")
                st.caption("Bron: PensioenPro transitieoverzicht 27 april 2026. "
                           "Negatieve verschuiving = invaardatum vervroegd.")

                tbl = uitstel_df[['name', 'oud', 'nieuw', 'Verschuiving (mnd)',
                                   'aum_euro_bn', 'wtp_contract_type', 'uitvoerder']].copy()
                tbl = tbl.rename(columns={
                    'name': 'Fonds', 'oud': 'Oorspronkelijke datum', 'nieuw': 'Nieuwe datum',
                    'aum_euro_bn': 'AUM (€ Bn)', 'wtp_contract_type': 'Contract',
                    'uitvoerder': 'Uitvoerder',
                })
                tbl = tbl.sort_values(['Verschuiving (mnd)', 'Nieuwe datum'], ascending=[False, True])
                st.dataframe(
                    tbl, use_container_width=True, hide_index=True,
                    column_config={
                        'Oorspronkelijke datum': st.column_config.DateColumn('Oorspr. datum', format='D MMM YYYY'),
                        'Nieuwe datum': st.column_config.DateColumn('Nieuwe datum', format='D MMM YYYY'),
                        'AUM (€ Bn)': st.column_config.NumberColumn('AUM (€ Bn)', format='%.1f'),
                    },
                )

        # --- Invaren timeline: who when, sized by AUM ---
        timeline_df = wtp_df.dropna(subset=['wtp_transitie_datum']).copy()
        timeline_df['date'] = pd.to_datetime(timeline_df['wtp_transitie_datum'], errors='coerce')
        timeline_df['oorspr'] = pd.to_datetime(timeline_df['wtp_oorspr_datum'], errors='coerce')
        timeline_df = timeline_df.dropna(subset=['date']).copy()
        if not timeline_df.empty:
            today_dt = pd.Timestamp.now(tz='UTC').tz_localize(None).normalize()
            timeline_df['is_past'] = timeline_df['date'] <= today_dt
            # Uitgesteld = oorspr. datum bekend en eerder dan huidige (positieve
            # verschuiving). 'Ingevaren' (datum <= vandaag) wint van 'Uitgesteld'.
            timeline_df['status'] = 'Gepland'
            timeline_df.loc[
                timeline_df['oorspr'].notna() & (timeline_df['oorspr'] < timeline_df['date']),
                'status'
            ] = 'Uitgesteld'
            timeline_df.loc[timeline_df['is_past'], 'status'] = 'Ingevaren'
            # Marker size: sqrt-scaling demt de ABP/PFZW-bias. Onbekende AUM krijgt
            # een vaste kleine waarde maar wordt apart benoemd in de hover.
            timeline_df['aum_known'] = timeline_df['aum_euro_bn'].notna()
            timeline_df['aum_marker'] = np.sqrt(timeline_df['aum_euro_bn'].fillna(1.0))
            timeline_df['AUM (€ Bn)'] = timeline_df['aum_euro_bn'].apply(
                lambda v: f"{v:.1f}" if pd.notna(v) else "?"
            )
            timeline_df['Oorspr. datum'] = timeline_df['oorspr'].dt.strftime('%d %b %Y').fillna('—')
            timeline_df['Uitvoerder'] = timeline_df['uitvoerder'].fillna('—')
            timeline_df['Contract'] = timeline_df['wtp_contract_type'].fillna('—')
            timeline_df = timeline_df.sort_values('date')

            counts = timeline_df['status'].value_counts()
            st.markdown(
                f"### Invaren-tijdlijn — "
                f"{counts.get('Ingevaren', 0)} ingevaren · "
                f"{counts.get('Uitgesteld', 0)} uitgesteld · "
                f"{counts.get('Gepland', 0)} origineel gepland"
            )
            fig_tl = px.scatter(
                timeline_df, x='date', y='status', color='status',
                size='aum_marker', hover_name='name',
                category_orders={'status': ['Ingevaren', 'Uitgesteld', 'Gepland']},
                hover_data={
                    'AUM (€ Bn)': True, 'Contract': True,
                    'Oorspr. datum': True, 'Uitvoerder': True,
                    'aum_marker': False, 'date': '|%d %b %Y', 'status': False,
                },
                color_discrete_map={'Ingevaren': GREEN_FILL, 'Gepland': ACCENT, 'Uitgesteld': ORANGE_FILL},
                labels={'date': '', 'status': ''},
                size_max=28,
            )
            # Today line — annotation added separately because plotly's
            # add_vline annotation path calls sum(x) with int start, which
            # rejects both pd.Timestamp and date strings on current versions.
            today = pd.Timestamp.now(tz='UTC').tz_localize(None).strftime('%Y-%m-%d')
            fig_tl.add_vline(x=today, line_dash='dot', line_color=MUTE_LINE)
            fig_tl.add_annotation(x=today, y=1, yref='paper', showarrow=False,
                                  text='vandaag', font=dict(color=MUTE_LINE),
                                  yanchor='bottom')
            fig_tl.update_layout(height=320, showlegend=True)
            show_chart(fig_tl)
        else:
            st.info("Geen geldige transitiedatums voor de tijdlijn.")

        def _format_wtp_table(df, asc):
            t = df.sort_values('sort_date', ascending=asc).drop(columns=['sort_date']).copy()
            t['wtp_transitie_datum'] = pd.to_datetime(t['wtp_transitie_datum'], errors='coerce')
            t['wtp_oorspr_datum'] = pd.to_datetime(t['wtp_oorspr_datum'], errors='coerce')
            return t.rename(columns={
                'name': 'Fonds',
                'wtp_transitie_datum': 'Transitiedatum',
                'wtp_oorspr_datum': 'Oorspr. datum',
                'wtp_contract_type': 'Contract',
                'wtp_invaren': 'Invaren',
                'aum_euro_bn': 'AUM (€ Bn)',
                'uitvoerder': 'Uitvoerder',
            })[['Fonds', 'Transitiedatum', 'Oorspr. datum', 'Contract',
                'Invaren', 'AUM (€ Bn)', 'Uitvoerder']]

        _date_cfg = {
            'Transitiedatum': st.column_config.DateColumn('Transitiedatum', format='D MMM YYYY'),
            'Oorspr. datum': st.column_config.DateColumn('Oorspr. datum', format='D MMM YYYY'),
            'AUM (€ Bn)': st.column_config.NumberColumn('AUM (€ Bn)', format='%.1f'),
        }

        st.subheader(f"🚀 Reeds Ingevaren ({len(past_df)} fondsen)")
        if not past_df.empty:
            st.dataframe(_format_wtp_table(past_df, asc=False),
                         use_container_width=True, hide_index=True,
                         column_config=_date_cfg)
        else:
            st.info("Nog geen fondsen geregistreerd als ingevaren.")

        st.subheader(f"📅 Geplande Transities ({len(future_df)} fondsen)")
        if not future_df.empty:
            st.dataframe(_format_wtp_table(future_df, asc=True),
                         use_container_width=True, hide_index=True,
                         column_config=_date_cfg)
        else:
            st.info("Alle fondsen zijn ingevaren.")

        if not no_transition_df.empty:
            st.subheader(f"🛑 Geen transitie / blijft in oude regeling ({len(no_transition_df)} fondsen)")
            st.caption("Fondsen die naar verzekeraar gaan, in ftk blijven of nog geen transitiedatum hebben.")
            st.dataframe(
                no_transition_df.drop(columns=['sort_date']).rename(columns={
                    'name': 'Fonds', 'wtp_contract_type': 'Contract',
                    'wtp_invaren': 'Invaren', 'aum_euro_bn': 'AUM (€ Bn)',
                    'uitvoerder': 'Uitvoerder',
                })[['Fonds', 'Contract', 'Invaren', 'AUM (€ Bn)', 'Uitvoerder']],
                use_container_width=True, hide_index=True,
                column_config={'AUM (€ Bn)': st.column_config.NumberColumn('AUM (€ Bn)', format='%.1f')},
            )
    else:
        st.info("Nog geen WTP-transitiedata in de database beschikbaar.")

# ==========================================
# PAGE 5: DEKKINGSGRAAD ANALYSIS
# ==========================================
def page_dekkingsgraad():
    st.header("Dekkingsgraden")
    st.markdown("Analyse van de financiële gezondheid en dekkingsgraden van Nederlandse pensioenfondsen.")

    valid_df = df_funds.dropna(subset=['dekkingsgraad_pct']).copy()
    if not valid_df.empty:
        above_100 = (valid_df['dekkingsgraad_pct'] >= 100).sum()
        above_120 = (valid_df['dekkingsgraad_pct'] >= 120).sum()
        idx_top = valid_df['dekkingsgraad_pct'].idxmax()
        idx_bot = valid_df['dekkingsgraad_pct'].idxmin()
        render_kpi_row([
            kpi_card("Fondsen boven 100%", f"{above_100}",
                     sub=f"van {len(valid_df)} ({above_100/len(valid_df)*100:.0f}%)"),
            kpi_card("Fondsen boven 120%", f"{above_120}",
                     sub="indexatie-ready"),
            kpi_card("Hoogste dekkingsgraad", f"{valid_df.loc[idx_top, 'dekkingsgraad_pct']:.1f}%",
                     sub=str(valid_df.loc[idx_top, 'name'])[:28]),
            kpi_card("Laagste dekkingsgraad", f"{valid_df.loc[idx_bot, 'dekkingsgraad_pct']:.1f}%",
                     sub=str(valid_df.loc[idx_bot, 'name'])[:28]),
        ])
        st.divider()
    valid_df = valid_df.sort_values(by='dekkingsgraad_pct', ascending=False)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 10 gezondste fondsen")
        fig_top = px.bar(valid_df.head(10), x="name", y="dekkingsgraad_pct", title="Hoogste dekkingsgraden",
                         color="category", color_discrete_map=CATEGORY_COLORS)
        fig_top.update_layout(yaxis_title="Dekkingsgraad (%)", xaxis_title="Fonds", height=380)
        fig_top.add_hline(y=100, line_dash="dash", line_color=RED_LINE, annotation_text="100% minimum")
        show_chart(fig_top)

    with col2:
        st.subheader("Onderste 10 fondsen")
        fig_bottom = px.bar(valid_df.tail(10).sort_values(by='dekkingsgraad_pct', ascending=True),
                            x="name", y="dekkingsgraad_pct", title="Laagste dekkingsgraden",
                            color="category", color_discrete_map=CATEGORY_COLORS)
        fig_bottom.update_layout(yaxis_title="Dekkingsgraad (%)", xaxis_title="Fonds", height=380)
        fig_bottom.add_hline(y=100, line_dash="dash", line_color=RED_LINE, annotation_text="100% minimum")
        show_chart(fig_bottom)

    st.divider()

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Verdeling")
        fig_hist = px.histogram(valid_df, x="dekkingsgraad_pct", nbins=20, title="Verdeling van dekkingsgraden",
                                color="category", color_discrete_map=CATEGORY_COLORS)
        fig_hist.update_layout(xaxis_title="Dekkingsgraad (%)", yaxis_title="Aantal fondsen", height=380)
        fig_hist.add_vline(x=100, line_dash="dash", line_color=RED_LINE)
        show_chart(fig_hist)
    with c2:
        st.subheader("Gemiddelde per categorie")
        cat_avg = valid_df.groupby('category')['dekkingsgraad_pct'].mean().reset_index().sort_values('dekkingsgraad_pct', ascending=False)
        fig_cat = px.bar(cat_avg, x="category", y="dekkingsgraad_pct", title="Gemiddelde dekkingsgraad per categorie",
                         color="category", color_discrete_map=CATEGORY_COLORS)
        fig_cat.update_layout(xaxis_title="Categorie", yaxis_title="Gem. dekkingsgraad (%)", height=380, showlegend=False)
        show_chart(fig_cat)

        st.subheader("Volledige dekkingsgraad-tabel")
        st.dataframe(valid_df[['name', 'category', 'aum_euro_bn', 'dekkingsgraad_pct']].reset_index(drop=True), use_container_width=True)

# ==========================================
# PAGE 6: ESG & SFDR Tracker
# ==========================================
def page_esg_sfdr():
    st.header("ESG- en SFDR-analyse")
    st.markdown("Analyse van SFDR-artikelclassificaties (Sustainable Finance Disclosure Regulation) en EU-taxonomie binnen de pensioensector. Deze data is via LLM geëxtraheerd uit recent gepubliceerde jaarverslagen 2024.")

    sfdr_df = df_funds.dropna(subset=['sfdr_article']).copy()
    if not sfdr_df.empty:
        article_series = sfdr_df['sfdr_article'].astype(str).str.extract(r'(\d+)', expand=False)
        art_counts = article_series.value_counts().to_dict()
        a6 = int(art_counts.get('6', 0))
        a8 = int(art_counts.get('8', 0))
        a9 = int(art_counts.get('9', 0))
        tax_mean = sfdr_df['eu_taxonomy_pct'].dropna().mean() if 'eu_taxonomy_pct' in sfdr_df else None

        # Artikel-verdeling als inline-badges onder de titel
        article_badges = " ".join([
            badge(f"Art 6: {a6}", "gray"),
            badge(f"Art 8: {a8}", "blue"),
            badge(f"Art 9: {a9}", "green"),
        ])
        st.markdown(article_badges, unsafe_allow_html=True)

        render_kpi_row([
            kpi_card("Fondsen geclassificeerd", f"{len(sfdr_df)}",
                     sub=f"van {len(df_funds)} gevolgd"),
            kpi_card("Aandeel Artikel 8",
                     f"{a8/len(sfdr_df)*100:.0f}%",
                     sub=f"{a8} fondsen — promoot E/S-kenmerken"),
            kpi_card("Aandeel Artikel 9",
                     f"{a9/len(sfdr_df)*100:.0f}%",
                     sub=f"{a9} fondsen — duurzaam beleggingsdoel"),
            kpi_card("Gem. EU-taxonomie",
                     f"{tax_mean:.1f}%" if tax_mean is not None else "—",
                     sub="aligned over geclassificeerde fondsen"),
        ])
        st.divider()

    if not sfdr_df.empty:
        c1, c2 = st.columns(2)
        with c1:
            article_counts = sfdr_df['sfdr_article'].value_counts().reset_index()
            article_counts.columns = ['SFDR-artikel', 'Aantal fondsen']
            article_counts['SFDR-artikel'] = 'Artikel ' + article_counts['SFDR-artikel'].astype(str).str.extract(r'(\d+)', expand=False)
            fig_pie = px.pie(
                article_counts, names='SFDR-artikel', values='Aantal fondsen',
                title="SFDR-classificaties (2024)", hole=0.55,
                color='SFDR-artikel',
                color_discrete_map={"Artikel 6": MUTE_LINE, "Artikel 8": BLUE_FILL, "Artikel 9": GREEN_FILL},
            )
            fig_pie.update_traces(
                textinfo='value+percent',
                hovertemplate="<b>%{label}</b><br>%{value} fondsen<extra></extra>",
            )
            fig_pie.update_layout(height=380)
            show_chart(fig_pie)

        with c2:
            tax_df = sfdr_df.dropna(subset=['eu_taxonomy_pct']).copy()
            if not tax_df.empty:
                fig_hist = px.histogram(tax_df, x="eu_taxonomy_pct", nbins=20,
                                        title="Verdeling EU-taxonomie alignment (%)",
                                        color="category", color_discrete_map=CATEGORY_COLORS)
                fig_hist.update_layout(height=380)
                show_chart(fig_hist)
            else:
                st.info("Nog geen EU-taxonomie-percentages geëxtraheerd.")

        st.subheader("Geëxtraheerde regelgevingsdata")
        st.dataframe(sfdr_df[['name', 'category', 'aum_euro_bn', 'sfdr_article', 'eu_taxonomy_pct']].sort_values(by='eu_taxonomy_pct', ascending=False), use_container_width=True)
    else:
        st.info("Nog geen SFDR-data geëxtraheerd. De LLM-extractie loopt op de achtergrond.")

# ==========================================
# PAGE 7: INDUSTRY NEWS FEED
# ==========================================
def page_news():
    st.header("Sectornieuws")
    st.markdown("Nieuwsberichten direct gescraped van de websites van Nederlandse pensioenfondsen. Gebruik de filters om gericht te zoeken op aankondigingen als indexatie, premies of duurzaamheidswijzigingen.")
    
    df_news = get_latest_news()
    
    if not df_news.empty:
        # Proper Chronological Sorting of Dutch Dates
        months = {
            'januari': '01', 'februari': '02', 'maart': '03', 'april': '04',
            'mei': '05', 'juni': '06', 'juli': '07', 'augustus': '08',
            'september': '09', 'oktober': '10', 'november': '11', 'december': '12'
        }
        
        def parse_dutch_date(d_str):
            if not isinstance(d_str, str) or not d_str: return pd.NaT
            d_lower = d_str.lower().strip()
            
            # If it looks like a clean ISO fallback string (YYYY-MM-DD)
            if '-' in d_lower and len(d_lower) == 10 and d_lower.startswith('202'):
                try:
                    return pd.to_datetime(d_lower, errors='coerce')
                except:
                    pass
            
            for dut, num in months.items():
                if dut in d_lower:
                    d_lower = d_lower.replace(dut, f"-{num}-").replace(' ', '')
                    break
                    
            try:
                d_clean = re.sub(r'[^\d\-]', '', d_lower)  # Strip loose text
                return pd.to_datetime(d_clean, dayfirst=True, errors='coerce')
            except:
                return pd.NaT
                
        df_news['_dt'] = df_news['Date'].apply(parse_dutch_date)
        df_news = df_news.sort_values(by=['_dt', 'Date'], ascending=[False, False])

        # Top-of-page KPIs
        recent_cutoff = pd.Timestamp.now(tz='UTC').tz_localize(None) - pd.Timedelta(days=30)
        recent_mask = df_news['_dt'].fillna(pd.Timestamp(0)) >= recent_cutoff
        n_recent = int(recent_mask.sum())
        n_funds = df_news['Pension Fund'].nunique() if 'Pension Fund' in df_news.columns else 0
        n_cats = df_news['Category'].nunique() if 'Category' in df_news.columns else 0
        render_kpi_row([
            kpi_card("Koppen", f"{len(df_news):,}".replace(',', '.'),
                     sub="gescraped van fondswebsites"),
            kpi_card("Laatste 30 dagen", f"{n_recent:,}".replace(',', '.'),
                     sub=f"{n_recent/max(len(df_news),1)*100:.0f}% van totaal"),
            kpi_card("Fondsen gedekt", f"{n_funds}",
                     sub="unieke fondsbronnen"),
            kpi_card("Categorieën", f"{n_cats}",
                     sub="soorten pensioenfondsen"),
        ])

        df_news = df_news.drop(columns=['_dt'])

        # Horizontale filterbalk
        f1, f2, f3 = st.columns([2, 3, 1])
        with f1:
            cat_filter = st.multiselect("Categorie", df_news["Category"].dropna().unique(), placeholder="Alle categorieën")
        with f2:
            search_query = st.text_input("Zoek in koppen", placeholder="indexatie, premie, MVB, ...")
        with f3:
            recent_only = st.toggle("Alleen laatste 30 dagen", value=False)

        filtered_news = df_news.copy()
        if cat_filter:
            filtered_news = filtered_news[filtered_news["Category"].isin(cat_filter)]
        if search_query:
            filtered_news = filtered_news[filtered_news["Headline"].str.contains(search_query, case=False, na=False)]
        if recent_only:
            cutoff_disp = pd.Timestamp.now(tz='UTC').tz_localize(None) - pd.Timedelta(days=30)
            _dt_disp = filtered_news['Date'].apply(parse_dutch_date)
            filtered_news = filtered_news[_dt_disp.fillna(pd.Timestamp(0)) >= cutoff_disp]

        cap_col, rss_col = st.columns([5, 1])
        with cap_col:
            st.caption(f"**{len(filtered_news):,}** van **{len(df_news):,}** koppen getoond".replace(',', '.'))
        with rss_col:
            # RSS 2.0 export of current filter
            try:
                from xml.sax.saxutils import escape as _xesc
                items_xml = []
                for _, r in filtered_news.head(100).iterrows():
                    items_xml.append(
                        "<item>"
                        f"<title>{_xesc(str(r.get('Headline') or ''))}</title>"
                        f"<link>{_xesc(str(r.get('url') or ''))}</link>"
                        f"<pubDate>{_xesc(str(r.get('Date') or ''))}</pubDate>"
                        f"<category>{_xesc(str(r.get('Category') or ''))}</category>"
                        f"<dc:creator>{_xesc(str(r.get('Pension Fund') or ''))}</dc:creator>"
                        "</item>"
                    )
                rss_xml = (
                    '<?xml version="1.0" encoding="UTF-8"?>'
                    '<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/">'
                    '<channel>'
                    '<title>Dutch Pension Funds — News</title>'
                    '<link>https://pensioenfondsen.streamlit.app</link>'
                    '<description>Recent news headlines from Dutch pension fund websites</description>'
                    f'<lastBuildDate>{pd.Timestamp.now(tz="UTC").strftime("%a, %d %b %Y %H:%M:%S +0000")}</lastBuildDate>'
                    + "".join(items_xml)
                    + '</channel></rss>'
                ).encode("utf-8")
                st.download_button(
                    label="📰 RSS",
                    data=rss_xml,
                    file_name=f"pension_news_{pd.Timestamp.now(tz='UTC').tz_localize(None).date()}.xml",
                    mime="application/rss+xml",
                    use_container_width=True,
                    help="Download de huidige filter als RSS 2.0 XML (max 100 items).",
                )
            except Exception as e:
                st.caption(f"RSS niet beschikbaar ({e})")

        st.dataframe(
            filtered_news,
            use_container_width=True,
            column_config={
                "url": st.column_config.LinkColumn("Bron", display_text="Open link \u2192"),
                "Date": st.column_config.TextColumn("Datum"),
                "Pension Fund": st.column_config.TextColumn("Fonds"),
                "Headline": st.column_config.TextColumn("Kop"),
            },
            hide_index=True
        )
    else:
        st.info("Geen nieuwsartikelen in de database. Controleer dat de scraper heeft gedraaid.")
        
# ==========================================
# PAGE 8: GLOSSARY (BEGRIPPENLIJST)
# ==========================================
def page_begrippenlijst():
    st.header("📖 Begrippenlijst")
    st.markdown("Een handig overzicht van veelvoorkomende pensioentermen en concepten die in deze database (en in het dagelijkse nieuws) worden gebruikt.")
    
    st.markdown("""
    * **Indexatie / Toeslagverlening:** Het verhogen van de opgebouwde pensioenen (en vaak ook de pensioenuitkeringen) om ze aan te passen aan de inflatie of loonontwikkeling.
    * **Dekkingsgraad:** De financiële gezondheid van een pensioenfonds, uitgedrukt in een percentage. Het is de verhouding tussen het beschikbare vermogen en de pensioenverplichtingen (nu en in de toekomst). Ligt het onder 100%, dan is er een tekort.
    * **Beleidsdekkingsgraad:** Het gemiddelde van de actuele dekkingsgraden van de afgelopen 12 maanden. Deze is stabieler en wordt vaak gebruikt als formele graadmeter voor besluiten over bijvoorbeeld indexatie.
    * **Wtp (Wet toekomst pensioenen):** De nieuwe pensioenwet die in 2023 is ingegaan. Alle fondsen moeten in 2028 definitief overgestapt zijn op het nieuwe stelsel (premieregeling).
    * **Contract types Wtp:** Onder de nieuwe pensioenwet kiezen werkgevers/fondsen grofweg tussen twee nieuwe smaken pensioencontracten:
        * **1. Solidaire premieregeling (SPR):** Het beleggingsrisico wordt collectief (samen) gedeeld. Er is sprake van een 'solidariteitsreserve' om grote klappen op te vangen en het pensioen zo stabiel mogelijk te houden. Jong en oud beleggen samen.
        * **2. Flexibele premieregeling (FPR):** Er is een individueler pensioenpotje en deelnemers hebben vaak zelf meer keuze (bijvoorbeeld hoeveel beleggingsrisico ze willen nemen). Er is geen of een minder grote collectieve buffer.
    * **Invaren:** Het omzetten (omrekenen) van de in het verleden opgebouwde pensioenaanspraken naar persoonlijke pensioenvermogens in het nieuwe Wtp-stelsel.
    * **Rekenrente:** De rentevoet (door DNB vastgesteld) waarmee pensioenfondsen hun toekomstige verplichtingen contant moeten maken. Een lage rekenrente zorgt voor enorme (papieren) verplichtingen. ([Bekijk huidige DNB Rentetermijnstructuur (RTS)](https://www.dnb.nl/statistieken/dashboards/pensioenen/rentetermijnstructuur-rts/))
    * **UPO (Uniform Pensioenoverzicht):** Het jaarlijkse overzicht dat iedere werknemer ontvangt met daarin de status van het opgebouwde pensioen.
    * **Waardeoverdracht:** Het meenemen van je opgebouwde pensioenwaarde als je van werkgever wisselt en daardoor bij een ander pensioenfonds terechtkomt.
    * **OFP (Organisme voor de Financiering van Pensioenen):** Een Belgisch pensioenvehikel, vaak gebruikt door multinationals voor grensoverschrijdende pensioenuitvoering vanwege flexibele wetgeving. Het valt onder de Belgische toezichthouder (FSMA) in plaats van DNB.
    * **PPI (Premiepensioeninstelling):** Een pensioeninstelling die beschikbare premieregelingen uitvoert maar zélf geen risico's mag dragen (zoals langleven- of arbeidsongeschiktheidsrisico). Bij een PPI bouwt elke deelnemer een eigen pensioenkapitaal op via beleggingen.
    * **CPC (Certified Pension Consultant):** Een academisch opgeleide en gecertificeerde pensioenadviseur. Dit is een wettelijk beschermde titel in Nederland voor specialisten die werkgevers, ondernemingsraden en fondsen adviseren over complexe pensioenvraagstukken en pensioenrecht.
    * **CDC (Collective Defined Contribution):** Een collectieve premieregeling waarbij de werkgever een vaste premie betaalt en het beleggings- en renterisico volledig bij het collectief van de werknemers ligt (bijv. ING CDC, NN CDC).
    * **BPF (Bedrijfstakpensioenfonds):** Een pensioenfonds voor een gehele sector of specifieke bedrijfstak (zoals de bouw, zorg, of horeca). Deelname is voor werkgevers binnen die sector vaak wettelijk verplicht (verplichtstelling) om concurrentie op arbeidsvoorwaarden te voorkomen en zodoende schaalvoordelen te kunnen behalen.
    """)


# ==========================================
# PAGE: VRAAG HET DE DATA (text-to-SQL, self-hosted only)
# ==========================================
def page_ask_data():
    st.header("Vraag het de data")
    st.markdown(
        "Stel een vraag in gewone taal. Een lokaal taalmodel (Ollama op de "
        "mini) vertaalt 'm naar SQL en draait die **alleen-lezen** op de "
        "database. De gegenereerde query wordt altijd getoond, zodat je kunt "
        "controleren wat er gebeurde."
    )

    if not text2sql.ollama_available():
        st.warning(
            "Het taalmodel is nu niet bereikbaar (Ollama op de mini draait "
            "niet of is opgestart). Probeer het zo opnieuw."
        )
    else:
        _examples = [
            "Top 10 fondsen met de hoogste AUM",
            "Welke fondsen zijn nog niet ingevaren, gesorteerd op AUM?",
            "Gemiddelde beleidsdekkingsgraad per categorie",
            "Welke 5 fondsen hadden in 2024 het hoogste beleggingsrendement?",
        ]
        st.caption("Voorbeelden (klik om in te vullen):")
        _ecols = st.columns(len(_examples))
        for _i, _ex in enumerate(_examples):
            if _ecols[_i].button(_ex, key=f"ask_ex_{_i}", use_container_width=True):
                st.session_state.ask_q = _ex

        question = st.text_input(
            "Je vraag", key="ask_q",
            placeholder="bv. Welke fondsen beheren meer dan 10 miljard euro?",
        )

        if st.button("Vraag het", type="primary") and question.strip():
            with st.spinner("Genereren + uitvoeren…"):
                # Persist in session_state so the result survives reruns
                # (rendering only on the click-run is fragile).
                st.session_state.ask_result = text2sql.ask(question.strip())

        res = st.session_state.get("ask_result")
        if res:
            if res["sql"]:
                st.markdown("**Gegenereerde SQL:**")
                st.code(res["sql"], language="sql")
            if res["ok"]:
                _df = res["df"]
                st.caption(f"{len(_df)} rij(en)")
                st.dataframe(_df, use_container_width=True, hide_index=True)
                # Auto-grafiek bij een label-kolom + numerieke kolom, modest aantal rijen.
                try:
                    _num = _df.select_dtypes("number").columns.tolist()
                    _cat = [c for c in _df.columns if c not in _num]
                    if 0 < len(_df) <= 50 and _num and _cat:
                        st.bar_chart(_df.set_index(_cat[0])[_num[0]])
                except Exception:
                    pass
            elif res["error"]:
                st.error(res["error"])

        st.caption(
            "⚠ Antwoorden zijn machine-gegenereerd uit één SQL-query — "
            "controleer de getoonde SQL bij twijfel."
        )


# ==========================================
# PAGE 9: DATACURATIE (self-hosted only)
# ==========================================
def page_datacuratie():
    st.header("Datacuratie")
    st.markdown(
        "Handmatige correcties op de fondsentabel. Een correctie wordt als "
        "**override** opgeslagen in een aparte, lokale database "
        "(`app_state.db`) — de bron-DB `pension_funds.db` wordt **niet** "
        "aangepast. Zo overleeft je correctie elke scraper-run en `git pull`, "
        "zonder merge-conflicten. De override wordt bij het inladen over de "
        "bron-waarde heen gelegd."
    )

    # Friendly NL labels for the most-edited columns; unlisted columns fall
    # back to their raw name. (Keys must match funds.<column>.)
    _CURATIE_LABELS = {
        "aum_euro_bn": "AUM (€ mld)",
        "beleidsdekkingsgraad_pct": "Beleidsdekkingsgraad (%)",
        "dekkingsgraad_pct": "Actuele dekkingsgraad (%)",
        "maanddekkingsgraad_pct": "Maanddekkingsgraad (%)",
        "vereiste_dekkingsgraad_pct": "Vereiste dekkingsgraad (%)",
        "deelnemers_totaal": "Deelnemers totaal",
        "deelnemers_actief": "Deelnemers actief",
        "deelnemers_slapers": "Deelnemers slapers",
        "deelnemers_gepensioneerd": "Deelnemers gepensioneerd",
        "uitvoerder": "Uitvoerder",
        "fiduciair_beheerder": "Fiduciair beheerder",
        "sfdr_article": "SFDR-artikel",
        "eu_taxonomy_pct": "EU-taxonomie (%)",
        "wtp_contract_type": "Wtp-contracttype",
        "wtp_invaren": "Wtp invaren",
        "wtp_transitie_datum": "Wtp-transitiedatum",
        "website": "Website",
        "category": "Categorie",
    }
    _editable = local_state.editable_columns()

    def _col_label(c: str) -> str:
        return f"{_CURATIE_LABELS.get(c, c)}  ·  {c}"

    df_sorted = df_funds.sort_values("name")
    fund_name = st.selectbox(
        "Fonds",
        df_sorted["name"].tolist(),
        key="curatie_fund",
    )
    fund_row = df_sorted[df_sorted["name"] == fund_name].iloc[0]
    fund_id = int(fund_row["id"])

    # Pull the raw bron-DB row so "huidige waarde" reflects the source, not the
    # override-merged df_funds.
    bron_row = load_data(
        "SELECT * FROM funds WHERE id = " + str(fund_id)
    )
    bron = bron_row.iloc[0] if not bron_row.empty else None

    col_l, col_r = st.columns([1, 1])

    with col_l:
        st.markdown("##### Veld bewerken")
        # Order the dropdown so the friendly-labelled columns come first.
        _ordered = [c for c in _CURATIE_LABELS if c in _editable] + \
                   [c for c in _editable if c not in _CURATIE_LABELS]
        chosen_col = st.selectbox(
            "Veld", _ordered, format_func=_col_label, key="curatie_col"
        )

        bron_val = None if bron is None else bron.get(chosen_col)
        bron_disp = "—" if (bron_val is None or pd.isna(bron_val)) else bron_val
        st.caption(f"Huidige bron-waarde: **{bron_disp}**")

        # Show any existing override on this field.
        _ov = local_state.get_overrides()
        _ov_here = _ov[(_ov.fund_id == fund_id) & (_ov.column_name == chosen_col)]
        if not _ov_here.empty:
            _r = _ov_here.iloc[0]
            _ovd = "NULL" if int(_r["is_null"]) else _r["new_value"]
            st.info(f"Actieve override: **{_ovd}**")

        with st.form("curatie_form", clear_on_submit=False):
            set_null = st.checkbox("Zet op NULL (leegmaken)", value=False)
            new_val = st.text_input(
                "Nieuwe waarde",
                value="" if (bron_val is None or pd.isna(bron_val)) else str(bron_val),
                disabled=set_null,
            )
            null_only = st.checkbox(
                "NULL-only-guard (alleen lege bron-velden invullen)",
                value=True,
                help="Aan: weigert overschrijven van een bestaande bron-waarde. "
                     "Zet uit om bewust te corrigeren (bv. KPN AUM 1,1 → 10,0).",
            )
            note = st.text_input("Notitie (optioneel)", value="")
            submitted = st.form_submit_button("Override opslaan", type="primary")

        if submitted:
            ok, msg = local_state.set_override(
                fund_id,
                chosen_col,
                None if set_null else new_val,
                is_null=set_null,
                note=note,
                null_only=null_only,
            )
            if ok:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)

    with col_r:
        st.markdown("##### Actieve overrides")
        ov_all = local_state.list_overrides_with_names()
        if ov_all.empty:
            st.caption("Nog geen overrides.")
        else:
            for _, r in ov_all.iterrows():
                disp = "NULL" if int(r["is_null"]) else r["new_value"]
                c1, c2, c3 = st.columns([3, 1, 1])
                c1.markdown(
                    f"**{r['name']}** · `{r['column_name']}` → {disp}"
                    f"<br><span style='font-size:11px;color:#8F9BA8;'>"
                    f"{r['updated_ts']}"
                    f"{(' · ' + r['note']) if r['note'] else ''}</span>",
                    unsafe_allow_html=True,
                )
                if c2.button("Wis", key=f"clr_{r['fund_id']}_{r['column_name']}"):
                    _ok, _m = local_state.clear_override(
                        int(r["fund_id"]), r["column_name"]
                    )
                    st.toast(_m)
                    st.rerun()
                if c3.button(
                    "Promoot", key=f"prm_{r['fund_id']}_{r['column_name']}",
                    help="Schrijf permanent naar pension_funds.db (alleen als de "
                         "pipeline deze kolom niet meer overschrijft).",
                ):
                    _ok, _m = local_state.promote_override(
                        int(r["fund_id"]), r["column_name"]
                    )
                    st.cache_data.clear()  # bron-DB gewijzigd → cache verversen
                    (st.success if _ok else st.error)(_m)
                    st.rerun()

    with st.expander("Wijzigingslog (laatste 50)"):
        audit = local_state.get_audit(limit=50)
        if audit.empty:
            st.caption("Nog geen wijzigingen geregistreerd.")
        else:
            st.dataframe(audit, use_container_width=True, hide_index=True)


# ==========================================
# NAVIGATIE (st.navigation — echte URL's per pagina, gegroepeerde sidebar)
# ==========================================
_PG_OVERVIEW = st.Page(page_sector_overview, title="Sectoroverzicht",
                       icon="🏠", default=True)
_PG_DEEP_DIVE = st.Page(page_fund_deep_dive, title="Fonds-diepteanalyse",
                        icon="🔍", url_path="fonds")
_PG_COMPARE = st.Page(page_fund_comparison, title="Fondsvergelijking",
                      icon="⚖️", url_path="vergelijk")
_PG_TRENDS = st.Page(page_trends, title="Trends", icon="📈", url_path="trends")
_PG_NEWS = st.Page(page_news, title="Sectornieuws", icon="📰", url_path="nieuws")
_PG_DEKKING = st.Page(page_dekkingsgraad, title="Dekkingsgraad-analyse",
                      icon="📊", url_path="dekkingsgraad")
_PG_EQUITY = st.Page(page_equity_strategy, title="Aandelenstrategie",
                     icon="🎯", url_path="aandelenstrategie")
_PG_MANAGERS = st.Page(page_asset_managers, title="Vermogensbeheerders",
                       icon="🏛️", url_path="beheerders")
_PG_BENCHMARKS = st.Page(page_benchmarks, title="Benchmarks",
                         icon="📏", url_path="benchmarks")
_PG_WTP = st.Page(page_wtp_tracker, title="WTP-tracker", icon="🔄", url_path="wtp")
_PG_ESG = st.Page(page_esg_sfdr, title="ESG- en SFDR-tracker",
                  icon="🌱", url_path="esg")
_PG_BEGRIPPEN = st.Page(page_begrippenlijst, title="Begrippenlijst",
                        icon="📖", url_path="begrippen")

_NAV = {
    "Overzicht": [_PG_OVERVIEW, _PG_TRENDS, _PG_NEWS],
    "Fondsen": [_PG_DEEP_DIVE, _PG_COMPARE],
    "Analyse": [_PG_DEKKING, _PG_EQUITY, _PG_MANAGERS, _PG_BENCHMARKS],
    "Transitie & ESG": [_PG_WTP, _PG_ESG],
    "Hulpmiddelen": [_PG_BEGRIPPEN],
}
# Self-hosted-only pagina's
if _TEXT2SQL_OK:
    _NAV["Hulpmiddelen"].append(
        st.Page(page_ask_data, title="Vraag het de data", icon="💬", url_path="vraag"))
if _LOCAL_STATE_OK:
    _NAV["Hulpmiddelen"].append(
        st.Page(page_datacuratie, title="Datacuratie", icon="🛠️", url_path="curatie"))

_nav = st.navigation(_NAV)

# Deep-links: /fonds?fund=X (en oude /?fund=X-links) openen de diepteanalyse.
if "fund" in st.query_params:
    _qp_fund = st.query_params["fund"]
    st.query_params.clear()
    if _qp_fund in set(df_funds["name"]):
        st.session_state.selected_fund = _qp_fund
        st.switch_page(_PG_DEEP_DIVE)

# Pending jump uit widget-callbacks (globale zoeker, recente fondsen, preview)
_jump = st.session_state.pop("_jump_fund", None)
if _jump:
    st.session_state.selected_fund = _jump
    st.switch_page(_PG_DEEP_DIVE)

_nav.run()
