import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.io as pio
import os
import urllib.parse
import re
import html as _html

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Dutch Pension Funds Explorer",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- VISUAL STYLE (inject style.css next to this file) ---
_STYLE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "style.css")
if os.path.exists(_STYLE_PATH):
    with open(_STYLE_PATH) as _f:
        st.markdown(f"<style>{_f.read()}</style>", unsafe_allow_html=True)

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

# Plotly defaults aligned with the CSU palette
pio.templates["csu_light"] = pio.templates["simple_white"]
pio.templates["csu_light"].layout.update(
    font=dict(family="-apple-system, BlinkMacSystemFont, Segoe UI, Helvetica Neue, Arial, sans-serif",
              color="#17202A", size=12),
    colorway=["#6554A3", "#1F6FB2", "#17756B", "#2F7D57", "#C66B16", "#B13B3B", "#8F9BA8"],
    paper_bgcolor="#FFFFFF",
    plot_bgcolor="#FFFFFF",
    margin=dict(l=10, r=10, t=40, b=10),
    title=dict(font=dict(size=13, color="#5C6875")),
)
pio.templates.default = "csu_light"

# Process cross-page URL query parameters
if "fund" in st.query_params:
    st.session_state.selected_fund = st.query_params["fund"]
    st.session_state.page = "Fund Deep-Dive"
    st.query_params.clear()

# --- DATABASE CONNECTION ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

def load_data(query):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# --- DATA FETCHING ---
def get_all_funds():
    query = """
    SELECT 
        f.id, f.name, f.category, f.aum_euro_bn, 
        COALESCE(f.maanddekkingsgraad_pct, f.dekkingsgraad_pct) AS dekkingsgraad_pct, 
        f.beleidsdekkingsgraad_pct,
        f.equity_allocation_pct, f.uitvoerder, f.deelnemers_totaal, f.website,
        f.deelnemers_actief, f.deelnemers_slapers, f.deelnemers_gepensioneerd,
        f.sfdr_article, f.eu_taxonomy_pct, f.investment_beliefs,
        e.co2_reduction_goal, e.sfdr_classification
    FROM funds f
    LEFT JOIN fund_esg_metrics e ON f.id = e.fund_id
    """
    return load_data(query)

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

def get_metrics_history(fund_id):
    """Multi-year metrics for a fund.

    Primary source is historical_metrics (rich, curated). Falls back to
    fy_annual_metrics for fiscal years that aren't yet in historical_metrics
    — so freshly-parsed jaarverslag values (e.g. FY2025 for KPN) appear on
    the chart without needing a separate backfill step.
    """
    query_main = f"""
    SELECT year, aum_euro_bn, economische_dekkingsgraad_pct, nominale_dekkingsgraad_pct,
           beleidsdekkingsgraad_pct, reele_dekkingsgraad_pct,
           beleggingsrendement_pct, indexatieverlening_pct, cpi_pct,
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
st.title("🇳🇱 Dutch Pension Funds Dashboard")
st.markdown("Interactive exploration of the Dutch pension sector (AUM, Allocations, ESG, and WTP Transitions).")

# Retrieve core dataset
df_funds = get_all_funds()

# Session State Initialization
if "page" not in st.session_state:
    st.session_state.page = "Sector Overview"

if "selected_fund" not in st.session_state:
    st.session_state.selected_fund = None

pages = ["Sector Overview", "Fund Deep-Dive", "Equity Strategy Deep-Dive", "Asset Managers Exposure", "WTP Tracker", "Dekkingsgraad Analysis", "ESG & SFDR Tracker", "Industry News Feed", "Begrippenlijst"]

# Sidebar Navigation — subtle, no loud titles
st.sidebar.radio(" ", pages, key="page", label_visibility="collapsed")

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
    <div><strong>{len(df_funds)}</strong> funds tracked</div>
    <div>Total AUM <strong>€{df_funds['aum_euro_bn'].sum():,.1f} Bn</strong></div>
    <div>DNB coverage <strong>{_dnb_count}</strong> funds</div>
    <div>Latest DNB quarter <strong>{_dnb_quarter}</strong></div>
  </div>
</div>
""",
    unsafe_allow_html=True,
)


# ==========================================
# PAGE 1: SECTOR OVERVIEW
# ==========================================
if st.session_state.page == "Sector Overview":
    st.header("Sector Overview")
    
    valid_aum = df_funds.dropna(subset=['aum_euro_bn'])
    valid_ratio = df_funds.dropna(subset=['dekkingsgraad_pct'])
    largest_row = valid_aum.loc[valid_aum['aum_euro_bn'].idxmax()]
    pct_aum_coverage = len(valid_aum) / len(df_funds) * 100

    render_kpi_row([
        kpi_card("Total AUM Tracked", f"€{valid_aum['aum_euro_bn'].sum():,.1f} Bn",
                 sub=f"across {len(valid_aum)} funds ({pct_aum_coverage:.0f}%)"),
        kpi_card("Avg. Dekkingsgraad", f"{valid_ratio['dekkingsgraad_pct'].mean():.1f}%",
                 sub=f"site-reported, {len(valid_ratio)} funds"),
        kpi_card("Largest Fund", str(largest_row['name'])[:24],
                 sub=f"€{largest_row['aum_euro_bn']:,.1f} Bn"),
        kpi_card("Funds Tracked", f"{len(df_funds)}",
                 sub=f"{df_funds['category'].nunique()} categories"),
    ])
    st.divider()
    
    # Charts Row
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("AUM vs Funding Ratio")
        # Scatter Plot
        fig_scatter = px.scatter(
            df_funds.dropna(subset=['aum_euro_bn', 'dekkingsgraad_pct']), 
            x="dekkingsgraad_pct", y="aum_euro_bn", 
            color="category", hover_name="name",
            labels={"dekkingsgraad_pct": "Actuele Dekkingsgraad (Site %)", "aum_euro_bn": "AUM (Billion €)"},
            log_y=True, # Log scale because ABP/PFZW skew the Y axis massively
            title="Log(AUM) vs Actuele Dekkingsgraad (Site)"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
        
    with c2:
        st.subheader("Market Share by Category")
        market_share = df_funds.groupby('category')['aum_euro_bn'].sum().reset_index()
        fig_pie = px.pie(market_share, values='aum_euro_bn', names='category', title="Total AUM Distribution")
        st.plotly_chart(fig_pie, use_container_width=True)
        
    st.divider()
    st.subheader("Fund Directory")
    st.markdown("Select a row to preview the fund inline, or use the link column to jump to its Deep-Dive page.")

    df_display = df_funds[['name', 'category', 'aum_euro_bn', 'dekkingsgraad_pct', 'equity_allocation_pct', 'uitvoerder']].copy()
    df_display.insert(0, 'Profile Link', df_display['name'].apply(lambda x: f"/?fund={urllib.parse.quote_plus(x)}"))

    selection = st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key="fund_directory_table",
        column_config={
            "Profile Link": st.column_config.LinkColumn(
                "Action",
                help="Click to view the Deep-Dive profile.",
                display_text="Deep-Dive →",
            ),
            "name": st.column_config.TextColumn("Fund"),
            "category": st.column_config.TextColumn("Category"),
            "aum_euro_bn": st.column_config.NumberColumn("AUM (€ Bn)", format="%.1f"),
            "dekkingsgraad_pct": st.column_config.NumberColumn("Dekkingsgraad", format="%.1f%%"),
            "equity_allocation_pct": st.column_config.NumberColumn("Equity %", format="%.1f%%"),
            "uitvoerder": st.column_config.TextColumn("Uitvoerder"),
        },
    )

    sel_rows = (selection.selection or {}).get("rows") or []
    if sel_rows:
        row = df_display.iloc[sel_rows[0]]
        fund_row = df_funds[df_funds['name'] == row['name']].iloc[0]

        cat = str(fund_row.get('category') or 'Unknown')
        cat_color = {
            "Tak": "purple", "Bedrijf": "blue", "Beroep": "teal",
            "Verzekeraar": "orange", "APF": "gray", "PPI": "outline",
            "Algemeen Pensioenfonds (Kring)": "blue",
        }.get(cat, "gray")

        sfdr_html = badge("SFDR not extracted", "outline")
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
    {badge(f"Uitvoerder: {fund_row.get('uitvoerder') or 'Unknown'}", "outline")}
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
            kpi_card("Equity Allocation",
                     f"{fund_row['equity_allocation_pct']:.1f}%" if pd.notnull(fund_row['equity_allocation_pct']) else "—"),
        ])

        def _open_deep_dive():
            st.session_state.selected_fund = str(row['name'])
            st.session_state.page = "Fund Deep-Dive"

        b1, b2 = st.columns([1, 5])
        with b1:
            st.button("Open Deep-Dive →", on_click=_open_deep_dive, key="open_dd_from_directory")
        with b2:
            if pd.notnull(fund_row.get('website')) and fund_row['website']:
                st.markdown(f"🌐 [{fund_row['website']}]({fund_row['website']})")

# ==========================================
# PAGE 2: FUND DEEP-DIVE
# ==========================================
elif st.session_state.page == "Fund Deep-Dive":
    st.header("Fund Profile Deep-Dive")
    st.markdown("Explore detailed metrics, historical performance, and recent news for Dutch pension funds.")
    
    # Fund Selector (Exclude APG as it is an asset manager)
    deep_dive_funds = df_funds[~df_funds['name'].isin(['APG', 'ASR', 'ASR PPI', 'Allianz', 'Allianz PPI', 'A.S. Watson Nederland'])]
    fund_names = deep_dive_funds['name'].sort_values().tolist()
    
    # Try to initialize the selectbox with the globally selected fund
    default_index = 0
    if st.session_state.selected_fund and st.session_state.selected_fund in fund_names:
        default_index = fund_names.index(st.session_state.selected_fund)
        
    def update_selected_fund():
        st.session_state.selected_fund = st.session_state.fund_selector_ui
        
    selected_fund_name = st.selectbox(
        "Search for a Pension Fund:", 
        fund_names, 
        index=default_index,
        key="fund_selector_ui",
        on_change=update_selected_fund
    )
    
    if selected_fund_name:
        fund_data = df_funds[df_funds['name'] == selected_fund_name].iloc[0]
        fund_id = fund_data['id']
        
        st.subheader(fund_data['name'])
        
        if 'description' in fund_data and pd.notnull(fund_data['description']) and fund_data['description'] != "":
            st.info(fund_data['description'])
            
        if pd.notnull(fund_data['website']) and fund_data['website'] != "":
            st.markdown(f"🌐 **Website:** [{fund_data['website']}]({fund_data['website']})")
            
        # Investment Beliefs moved to ESG section
        render_kpi_row([
            kpi_card("AUM",
                     f"€{fund_data['aum_euro_bn']:,.1f} Bn" if pd.notnull(fund_data['aum_euro_bn']) else "—",
                     sub=str(fund_data.get('category') or '')),
            kpi_card("Actuele Dekkingsgraad",
                     f"{fund_data['dekkingsgraad_pct']:.1f}%" if pd.notnull(fund_data['dekkingsgraad_pct']) else "—",
                     sub="site-reported"),
            kpi_card("Equity Allocation",
                     f"{fund_data['equity_allocation_pct']:.1f}%" if pd.notnull(fund_data['equity_allocation_pct']) else "—",
                     sub="of total portfolio"),
            kpi_card("Participants",
                     f"{fund_data['deelnemers_totaal']:,.0f}".replace(",", ".") if pd.notnull(fund_data['deelnemers_totaal']) else "—",
                     sub="actief + slapers + gepensioneerd"),
        ])

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
  <div class="section-card-title">Latest annual report · FY {latest_fy} &nbsp; {link_html}</div>
</div>
""",
                unsafe_allow_html=True,
            )
            render_kpi_row([
                kpi_card("AUM (FY)",
                         f"€{fy_aum:,.1f} Bn" if fy_aum is not None else "—",
                         sub=f"from jaarverslag {latest_fy}"),
                kpi_card("Actuele Dekkingsgraad (FY)",
                         f"{fy_actu:.1f}%" if fy_actu is not None else "—",
                         sub=f"year-end {latest_fy}"),
                kpi_card("Beleidsdekkingsgraad (FY)",
                         f"{fy_beleid:.1f}%" if fy_beleid is not None else "—",
                         sub=f"year-end {latest_fy}"),
                kpi_card("Equity Allocation (FY)",
                         f"{fy_eq:.1f}%" if fy_eq is not None else "—",
                         sub=f"per jaarverslag {latest_fy}"),
            ])
            # Highlight values that disagree with the funds-table KPIs above
            mismatches = []
            if fy_aum is not None and pd.notnull(fund_data['aum_euro_bn']) and abs(fy_aum - fund_data['aum_euro_bn']) > 0.5:
                mismatches.append(f"AUM (funds table {fund_data['aum_euro_bn']:.1f} vs jaarverslag {fy_aum:.1f})")
            if fy_actu is not None and pd.notnull(fund_data['dekkingsgraad_pct']) and abs(fy_actu - fund_data['dekkingsgraad_pct']) > 1.0:
                mismatches.append(f"dekkingsgraad ({fund_data['dekkingsgraad_pct']:.1f}% vs {fy_actu:.1f}%)")
            if mismatches:
                st.caption("⚠ funds-table values differ from the annual report: " + "; ".join(mismatches))

        st.divider()

        col_main, col_side = st.columns([2, 1])
        
        with col_main:
            st.markdown("### Historical Performance")
            history_df = get_metrics_history(fund_id)
            if not history_df.empty:
                # Ensure columns are numeric to prevent Plotly Express wide-form data error
                for col in ["beleidsdekkingsgraad_pct", "beleggingsrendement_pct"]:
                    if col in history_df.columns:
                        history_df[col] = pd.to_numeric(history_df[col], errors='coerce')
                        
                fig_line = px.line(history_df, x="year", y=["beleidsdekkingsgraad_pct", "beleggingsrendement_pct"], 
                                   labels={"value": "Percentage (%)", "year": "Jaarverslag", "variable": "Metric"},
                                   title="Meerjarenoverzicht: Dekkingsgraad & Rendement (Jaarrapportages)")
                fig_line.update_xaxes(dtick=1, tickformat="d")
                st.plotly_chart(fig_line, use_container_width=True)
                
                st.markdown("#### Meerjarenoverzicht (Jaarrapportages)")
                
                rename_map = {
                    'aum_euro_bn': 'Belegd vermogen (€ mrd)',
                    'economische_dekkingsgraad_pct': 'Actuele dekkingsgraad',
                    'nominale_dekkingsgraad_pct': 'Nominale dekkingsgraad',
                    'beleidsdekkingsgraad_pct': 'Beleidsdekkingsgraad',
                    'reele_dekkingsgraad_pct': 'Reële dekkingsgraad',
                    'beleggingsrendement_pct': 'Totaal rendement',
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
                st.info("No historical metrics available for this fund.")
                
            st.markdown("### Recent News Articles")
            news_df = get_fund_news(fund_id)
            if not news_df.empty:
                for _, row in news_df.head(5).iterrows():
                    st.markdown(f"**{row['published_date']}** - [{row['title']}]({row['url']})")
            else:
                st.info("No recent news articles scraped.")
                
        with col_side:
            st.markdown("### Key Metrics")
            if pd.notnull(fund_data['deelnemers_totaal']):
                st.markdown(f"**Total Participants:** {fund_data['deelnemers_totaal']:,.0f}")
                st.markdown(f"- **Active:** {fund_data['deelnemers_actief']:,.0f}" if pd.notnull(fund_data['deelnemers_actief']) else "- **Active:** N/A")
                st.markdown(f"- **Sleepers:** {fund_data['deelnemers_slapers']:,.0f}" if pd.notnull(fund_data['deelnemers_slapers']) else "- **Sleepers:** N/A")
                st.markdown(f"- **Retired:** {fund_data['deelnemers_gepensioneerd']:,.0f}" if pd.notnull(fund_data['deelnemers_gepensioneerd']) else "- **Retired:** N/A")
            
            st.markdown("### Operations & ESG")
            st.markdown(f"**Administrator (Uitvoerder):** {fund_data['uitvoerder'] if pd.notnull(fund_data['uitvoerder']) else 'Unknown'}")
            
            # Show extracted SFDR metrics if available
            if pd.notnull(fund_data['sfdr_article']):
                article_num = str(int(fund_data['sfdr_article']))
                article_color = {"6": "gray", "8": "blue", "9": "green"}.get(article_num, "purple")
                article_html = badge(f"Article {article_num}", article_color)
            else:
                article_html = badge("Not extracted", "outline")
            tax_pct = f"{fund_data['eu_taxonomy_pct']}%" if pd.notnull(fund_data['eu_taxonomy_pct']) else "Not extracted"

            st.markdown(f"**Extracted SFDR Article (2024):** {article_html}", unsafe_allow_html=True)
            st.markdown(f"**Extracted EU Taxonomy %:** {tax_pct}")
            
            st.markdown(f"**Reported SFDR Classification:** {fund_data['sfdr_classification'] if pd.notnull(fund_data['sfdr_classification']) else 'Not Specified'}")
            st.markdown(f"**CO2 Goal:** {fund_data['co2_reduction_goal'] if pd.notnull(fund_data['co2_reduction_goal']) else 'Not Specified'}")
            
            st.markdown("### Equity Portfolio Managers")
            managers_df = get_fund_managers(fund_id)
            if not managers_df.empty:
                for mgr in managers_df['manager'].tolist():
                    st.markdown(f"- {mgr}")
            st.markdown("### Historical Annual Reports")
            reports_df = get_fund_reports(fund_id)
            if not reports_df.empty:
                for _, row in reports_df.iterrows():
                    st.markdown(f"- [{row['title']}]({row['url']})")
            else:
                st.write("No annual report links found in the database.")
                
            st.markdown("### ESG & Sustainability Reports")
            esg_reports_df = get_fund_esg_reports(fund_id)
            if not esg_reports_df.empty:
                for _, row in esg_reports_df.iterrows():
                    st.markdown(f"- [{row['title']}]({row['url']})")
            else:
                st.write("No specific sustainability reports extracted.")
                
            if 'investment_beliefs' in fund_data and pd.notnull(fund_data['investment_beliefs']) and fund_data['investment_beliefs'] != "":
                st.markdown(f"#### Investment Beliefs\n> {fund_data['investment_beliefs']}")

# ==========================================
# PAGE 2B: EQUITY STRATEGY DEEP-DIVE
# ==========================================
elif st.session_state.page == "Equity Strategy Deep-Dive":
    st.header("📈 Equity Strategy: Mid-Market (1-5 Bn)")
    st.markdown("Overview of the specific equity (stock) investments, strategy notes, and external managers for mid-sized pension funds (1 to 5 Billion AUM).")
    
    query_eq = """
    SELECT 
        f.name as "Pension Fund", 
        f.aum_euro_bn as "AUM (€ Bn)", 
        f.equity_allocation_pct as "Equity %", 
        f.equity_beheerkosten_pct as "Aandelen Beheerkosten %",
        f.equity_transactiekosten_pct as "Aandelen Transactie %",
        f.equity_performance_fee_mln as "Aandelen Perf. Fee (€ Mln)",
        f.equity_strategy_notes as "Strategy Notes",
        GROUP_CONCAT(e.fund_name, ', ') as "External Managers"
    FROM funds f
    LEFT JOIN equity_portfolio_funds e ON f.id = e.fund_id
    WHERE f.aum_euro_bn >= 1.0 AND f.aum_euro_bn <= 5.0
    GROUP BY f.id
    ORDER BY f.aum_euro_bn DESC
    """
    eq_df = load_data(query_eq)

    if not eq_df.empty:
        # KPI tiles for the cohort
        valid_eq = eq_df.dropna(subset=['Equity %'])
        valid_kosten = eq_df.dropna(subset=['Aandelen Beheerkosten %'])
        with_notes = eq_df['Strategy Notes'].notna() & (eq_df['Strategy Notes'].astype(str).str.strip() != '')
        with_managers = eq_df['External Managers'].notna() & (eq_df['External Managers'].astype(str).str.strip() != '')

        render_kpi_row([
            kpi_card("Funds in Cohort", f"{len(eq_df)}",
                     sub="€1–5 Bn AUM range"),
            kpi_card("Total AUM in Cohort",
                     f"€{eq_df['AUM (€ Bn)'].sum():,.1f} Bn",
                     sub=f"avg €{eq_df['AUM (€ Bn)'].mean():,.1f} Bn / fund"),
            kpi_card("Avg Equity Allocation",
                     f"{valid_eq['Equity %'].mean():.1f}%" if not valid_eq.empty else "—",
                     sub=f"{len(valid_eq)}/{len(eq_df)} disclose"),
            kpi_card("Avg Beheerkosten",
                     f"{valid_kosten['Aandelen Beheerkosten %'].mean():.3f}%" if not valid_kosten.empty else "—",
                     sub=f"{len(valid_kosten)}/{len(eq_df)} disclose"),
        ])
        st.markdown(
            " ".join([
                badge(f"With strategy notes: {int(with_notes.sum())}", "blue"),
                badge(f"With external managers: {int(with_managers.sum())}", "purple"),
                badge(f"Missing data: {len(eq_df) - int((with_notes | with_managers).sum())}", "outline"),
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
                "Equity %": st.column_config.NumberColumn("Equity %", format="%.1f%%"),
                "Aandelen Beheerkosten %": st.column_config.NumberColumn("Beheerkosten", format="%.3f%%"),
                "Aandelen Transactie %": st.column_config.NumberColumn("Transactiekosten", format="%.3f%%"),
                "Aandelen Perf. Fee (€ Mln)": st.column_config.NumberColumn("Perf. Fee (€ Mln)", format="%.2f"),
                "Strategy Notes": st.column_config.TextColumn("Strategy Notes", width="large"),
                "External Managers": st.column_config.TextColumn("External Managers", width="medium"),
            },
        )
    else:
        st.info("No funds found in the 1-5 Bn AUM range.")

# ==========================================
# PAGE 3: ASSET MANAGERS EXPOSURE
# ==========================================
elif st.session_state.page == "Asset Managers Exposure":
    st.header("External Asset Managers (Equity Portfolios)")
    st.markdown("Tracking which external investment firms manage the equity portfolios of Dutch pension funds.")

    query = """
    SELECT e.fund_name as Manager, COUNT(e.fund_id) as Number_Of_Pension_Clients
    FROM equity_portfolio_funds e
    GROUP BY e.fund_name
    ORDER BY Number_Of_Pension_Clients DESC
    LIMIT 20
    """
    managers_df = load_data(query)
    total_managers = load_data("SELECT COUNT(DISTINCT fund_name) AS n FROM equity_portfolio_funds")['n'][0]
    total_relations = load_data("SELECT COUNT(*) AS n FROM equity_portfolio_funds")['n'][0]
    top_mgr = managers_df.iloc[0] if not managers_df.empty else None

    render_kpi_row([
        kpi_card("Managers Tracked", f"{total_managers:,}".replace(',', '.'),
                 sub="distinct equity-portfolio manager names"),
        kpi_card("Total Relationships", f"{total_relations:,}".replace(',', '.'),
                 sub="manager × fund mandate rows"),
        kpi_card("Most-Used Manager", str(top_mgr['Manager'])[:24] if top_mgr is not None else "—",
                 sub=f"{int(top_mgr['Number_Of_Pension_Clients'])} pension clients" if top_mgr is not None else ""),
        kpi_card("Top-20 Coverage",
                 f"{managers_df['Number_Of_Pension_Clients'].sum() / max(total_relations,1) * 100:.0f}%" if total_relations else "—",
                 sub="share of all relationships in top 20"),
    ])
    st.divider()

    fig_bar = px.bar(managers_df, x="Manager", y="Number_Of_Pension_Clients",
                     title="Top 20 Asset Managers by Number of Dutch Pension Fund Clients",
                     labels={"Number_Of_Pension_Clients": "Client Count"})
    st.plotly_chart(fig_bar, use_container_width=True)

    st.dataframe(managers_df, use_container_width=True)

# ==========================================
# PAGE 4: WTP TRACKER
# ==========================================
elif st.session_state.page == "WTP Tracker":
    st.header("WTP Transition Tracker")
    st.markdown("Tracking the planned transition dates to the new pension system (Wet Toekomst Pensioenen).")

    query = """
    SELECT name, aum_euro_bn, wtp_transitie_datum, wtp_contract_type, wtp_invaren
    FROM funds
    WHERE wtp_transitie_datum IS NOT NULL
    """
    wtp_df = load_data(query)
    total_funds_in_db = len(df_funds)
    wtp_known = len(wtp_df)
    spr_count = (wtp_df['wtp_contract_type'].astype(str).str.upper().str.contains('SPR|SOLIDAIR', na=False)).sum() if not wtp_df.empty else 0
    fpr_count = (wtp_df['wtp_contract_type'].astype(str).str.upper().str.contains('FPR|FLEXIB', na=False)).sum() if not wtp_df.empty else 0
    invaren_done = (wtp_df['wtp_invaren'].astype(str).str.lower().isin(['ja','yes','true','ingevaren','done'])).sum() if not wtp_df.empty else 0

    render_kpi_row([
        kpi_card("WTP Plans Known", f"{wtp_known}",
                 sub=f"of {total_funds_in_db} tracked funds ({wtp_known/max(total_funds_in_db,1)*100:.0f}%)"),
        kpi_card("Already Ingevaren", f"{invaren_done}",
                 sub="funds with completed invaren"),
        kpi_card("Solidair (SPR)", f"{spr_count}",
                 sub=f"vs FPR: {fpr_count}"),
        kpi_card("Avg AUM in Scope",
                 f"€{wtp_df['aum_euro_bn'].mean():,.1f} Bn" if not wtp_df.empty and wtp_df['aum_euro_bn'].notna().any() else "—",
                 sub="among funds with WTP plan"),
    ])
    st.divider()
    
    if not wtp_df.empty:
        # Helper to convert Dutch string dates to sortable format YYYY-MM-DD
        def to_sortable_date(d_str):
            if not isinstance(d_str, str) or not d_str: 
                return "2099-12-31"  # Default to far future for empty values
            d = d_str.lower().strip()
            month_map = {
                'jan': '01', 'feb': '02', 'mrt': '03', 'apr': '04', 'mei': '05', 'jun': '06',
                'jul': '07', 'aug': '08', 'sep': '09', 'okt': '10', 'nov': '11', 'dec': '12'
            }
            parts = d.split('-')
            if len(parts) >= 3:
                day = parts[0].zfill(2)
                month = month_map.get(parts[1][:3], '01')
                year = "20" + parts[2] if len(parts[2]) == 2 else parts[2]
                return f"{year}-{month}-{day}"
            elif "20" in d: # e.g., "januari 2026"
                words = d.split()
                if len(words) == 2:
                    month = month_map.get(words[0][:3], '01')
                    return f"{words[1]}-{month}-01"
            return d_str

        # Helper to determine if a transition date is in the past (before March 2026)
        def is_past(d_str):
            if not isinstance(d_str, str) or not d_str: return False
            d = d_str.lower()
            if any(y in d for y in ['2023', '2024', '2025']): return True
            if any(m in d for m in ['jan-26', 'januari 2026', '2026-01']): return True
            return False
            
        wtp_df['sort_date'] = wtp_df['wtp_transitie_datum'].apply(to_sortable_date)
        wtp_df['is_past'] = wtp_df['wtp_transitie_datum'].apply(is_past)
        
        past_df = wtp_df[wtp_df['is_past']].drop(columns=['is_past'])
        future_df = wtp_df[~wtp_df['is_past']].drop(columns=['is_past'])
        
        c1, c2 = st.columns(2)
        with c1:
            timeline_counts = future_df['wtp_transitie_datum'].value_counts().reset_index()
            timeline_counts.columns = ['Transition Date', 'Number of Funds']
            timeline_counts['Sort Key'] = timeline_counts['Transition Date'].apply(to_sortable_date)
            timeline_counts = timeline_counts.sort_values('Sort Key').drop(columns=['Sort Key'])
            fig_bar = px.bar(timeline_counts, x='Transition Date', y='Number of Funds', title="Planned Transitions per Date")
            st.plotly_chart(fig_bar, use_container_width=True)
            
        with c2:
            contract_counts = wtp_df['wtp_contract_type'].value_counts().reset_index()
            contract_counts.columns = ['Contract Type', 'Count']
            fig_pie = px.pie(contract_counts, names='Contract Type', values='Count', title="All Contract Types (SPR vs FPR)")
            st.plotly_chart(fig_pie, use_container_width=True)
            
        st.subheader("🚀 Reeds Ingevaren (Transitie Voltooid)")
        if not past_df.empty:
            sorted_past = past_df.sort_values('sort_date', ascending=True).drop(columns=['sort_date'])
            st.dataframe(sorted_past, use_container_width=True, hide_index=True)
        else:
            st.info("Nog geen fondsen geregistreerd als ingevaren.")
            
        st.subheader("📅 Geplande Transities (Toekomst)")
        if not future_df.empty:
            sorted_future = future_df.sort_values('sort_date', ascending=True).drop(columns=['sort_date'])
            st.dataframe(sorted_future, use_container_width=True, hide_index=True)
        else:
            st.info("Alle fondsen zijn ingevaren.")
    else:
        st.info("No WTP transition data available in the database yet.")

# ==========================================
# PAGE 5: DEKKINGSGRAAD ANALYSIS
# ==========================================
elif st.session_state.page == "Dekkingsgraad Analysis":
    st.header("Funding Ratios (Dekkingsgraad)")
    st.markdown("Analysis of the financial health and funding ratios of Dutch pension funds.")

    valid_df = df_funds.dropna(subset=['dekkingsgraad_pct']).copy()
    if not valid_df.empty:
        above_100 = (valid_df['dekkingsgraad_pct'] >= 100).sum()
        above_120 = (valid_df['dekkingsgraad_pct'] >= 120).sum()
        idx_top = valid_df['dekkingsgraad_pct'].idxmax()
        idx_bot = valid_df['dekkingsgraad_pct'].idxmin()
        render_kpi_row([
            kpi_card("Funds Above 100%", f"{above_100}",
                     sub=f"of {len(valid_df)} ({above_100/len(valid_df)*100:.0f}%)"),
            kpi_card("Funds Above 120%", f"{above_120}",
                     sub="indexatie-ready territory"),
            kpi_card("Highest Ratio", f"{valid_df.loc[idx_top, 'dekkingsgraad_pct']:.1f}%",
                     sub=str(valid_df.loc[idx_top, 'name'])[:28]),
            kpi_card("Lowest Ratio", f"{valid_df.loc[idx_bot, 'dekkingsgraad_pct']:.1f}%",
                     sub=str(valid_df.loc[idx_bot, 'name'])[:28]),
        ])
        st.divider()
    valid_df = valid_df.sort_values(by='dekkingsgraad_pct', ascending=False)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 10 Healthiest Funds")
        fig_top = px.bar(valid_df.head(10), x="name", y="dekkingsgraad_pct", title="Highest Funding Ratios", color="category")
        fig_top.update_layout(yaxis_title="Funding Ratio (%)", xaxis_title="Fund")
        fig_top.add_hline(y=100, line_dash="dash", line_color="red", annotation_text="100% Minimum")
        st.plotly_chart(fig_top, use_container_width=True)
        
    with col2:
        st.subheader("Bottom 10 Funds")
        fig_bottom = px.bar(valid_df.tail(10).sort_values(by='dekkingsgraad_pct', ascending=True), 
                            x="name", y="dekkingsgraad_pct", title="Lowest Funding Ratios", color="category")
        fig_bottom.update_layout(yaxis_title="Funding Ratio (%)", xaxis_title="Fund")
        fig_bottom.add_hline(y=100, line_dash="dash", line_color="red", annotation_text="100% Minimum")
        st.plotly_chart(fig_bottom, use_container_width=True)
        
    st.divider()
    
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Distribution")
        fig_hist = px.histogram(valid_df, x="dekkingsgraad_pct", nbins=20, title="Distribution of Funding Ratios", color="category")
        fig_hist.update_layout(xaxis_title="Funding Ratio (%)", yaxis_title="Count of Funds")
        fig_hist.add_vline(x=100, line_dash="dash", line_color="red")
        st.plotly_chart(fig_hist, use_container_width=True)
    with c2:
        st.subheader("Average by Category")
        cat_avg = valid_df.groupby('category')['dekkingsgraad_pct'].mean().reset_index().sort_values('dekkingsgraad_pct', ascending=False)
        fig_cat = px.bar(cat_avg, x="category", y="dekkingsgraad_pct", title="Average Funding Ratio by Sector Category", color="category")
        fig_cat.update_layout(xaxis_title="Category", yaxis_title="Average Funding Ratio (%)")
        st.plotly_chart(fig_cat, use_container_width=True)
        
        st.subheader("Complete Dekkingsgraad Table")
        st.dataframe(valid_df[['name', 'category', 'aum_euro_bn', 'dekkingsgraad_pct']].reset_index(drop=True), use_container_width=True)

# ==========================================
# PAGE 6: ESG & SFDR Tracker
# ==========================================
elif st.session_state.page == "ESG & SFDR Tracker":
    st.header("ESG & SFDR Analysis")
    st.markdown("Analysis of Sustainable Finance Disclosure Regulation (SFDR) Article classifications and EU Taxonomy alignment across the pension sector. This data is extracted via LLM from recently published 2024 annual reports (Phase 22).")

    sfdr_df = df_funds.dropna(subset=['sfdr_article']).copy()
    if not sfdr_df.empty:
        article_series = sfdr_df['sfdr_article'].astype(str).str.extract(r'(\d+)', expand=False)
        art_counts = article_series.value_counts().to_dict()
        a6 = int(art_counts.get('6', 0))
        a8 = int(art_counts.get('8', 0))
        a9 = int(art_counts.get('9', 0))
        tax_mean = sfdr_df['eu_taxonomy_pct'].dropna().mean() if 'eu_taxonomy_pct' in sfdr_df else None

        # Article distribution as inline badges in the subtitle
        article_badges = " ".join([
            badge(f"Art 6: {a6}", "gray"),
            badge(f"Art 8: {a8}", "blue"),
            badge(f"Art 9: {a9}", "green"),
        ])
        st.markdown(article_badges, unsafe_allow_html=True)

        render_kpi_row([
            kpi_card("Funds Classified", f"{len(sfdr_df)}",
                     sub=f"of {len(df_funds)} tracked"),
            kpi_card("Article 8 Share",
                     f"{a8/len(sfdr_df)*100:.0f}%",
                     sub=f"{a8} funds — promotes E/S characteristics"),
            kpi_card("Article 9 Share",
                     f"{a9/len(sfdr_df)*100:.0f}%",
                     sub=f"{a9} funds — sustainable investment objective"),
            kpi_card("Avg EU Taxonomy",
                     f"{tax_mean:.1f}%" if tax_mean is not None else "—",
                     sub="alignment across classified funds"),
        ])
        st.divider()
    
    if not sfdr_df.empty:
        c1, c2 = st.columns(2)
        with c1:
            article_counts = sfdr_df['sfdr_article'].value_counts().reset_index()
            article_counts.columns = ['SFDR Article', 'Fund Count']
            article_counts['SFDR Article'] = 'Article ' + article_counts['SFDR Article'].astype(str).str.extract(r'(\d+)', expand=False)
            fig_pie = px.pie(article_counts, names='SFDR Article', values='Fund Count', title="SFDR Classifications (2024 Extract)")
            st.plotly_chart(fig_pie, use_container_width=True)
            
        with c2:
            tax_df = sfdr_df.dropna(subset=['eu_taxonomy_pct']).copy()
            if not tax_df.empty:
                fig_hist = px.histogram(tax_df, x="eu_taxonomy_pct", nbins=20, title="EU Taxonomy Alignment (%) Distribution", color="category")
                st.plotly_chart(fig_hist, use_container_width=True)
            else:
                st.info("No EU Taxonomy percentages extracted yet.")
                
        st.subheader("Extracted Regulatory Data")
        st.dataframe(sfdr_df[['name', 'category', 'aum_euro_bn', 'sfdr_article', 'eu_taxonomy_pct']].sort_values(by='eu_taxonomy_pct', ascending=False), use_container_width=True)
    else:
        st.info("No SFDR data has been extracted yet. The background LLM processing job is currently running.")

# ==========================================
# PAGE 7: INDUSTRY NEWS FEED
# ==========================================
elif st.session_state.page == "Industry News Feed":
    st.header("Industry News Feed")
    st.markdown("Latest news headlines scraped directly from the official websites of Dutch pension funds. Use the filters below to find specific announcements like indexations, premiums, or sustainability changes.")
    
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
            kpi_card("Headlines", f"{len(df_news):,}".replace(',', '.'),
                     sub="scraped from fund websites"),
            kpi_card("Last 30 Days", f"{n_recent:,}".replace(',', '.'),
                     sub=f"{n_recent/max(len(df_news),1)*100:.0f}% of total"),
            kpi_card("Funds Covered", f"{n_funds}",
                     sub="distinct fund sources"),
            kpi_card("Categories", f"{n_cats}",
                     sub="of pension fund types"),
        ])

        df_news = df_news.drop(columns=['_dt'])

        # Horizontal filter bar
        f1, f2, f3 = st.columns([2, 3, 1])
        with f1:
            cat_filter = st.multiselect("Category", df_news["Category"].dropna().unique(), placeholder="All categories")
        with f2:
            search_query = st.text_input("Search headlines", placeholder="indexatie, premie, MVB, ...")
        with f3:
            recent_only = st.checkbox("Last 30d only", value=False)

        filtered_news = df_news.copy()
        if cat_filter:
            filtered_news = filtered_news[filtered_news["Category"].isin(cat_filter)]
        if search_query:
            filtered_news = filtered_news[filtered_news["Headline"].str.contains(search_query, case=False, na=False)]
        if recent_only:
            cutoff_disp = pd.Timestamp.now(tz='UTC').tz_localize(None) - pd.Timedelta(days=30)
            _dt_disp = filtered_news['Date'].apply(parse_dutch_date)
            filtered_news = filtered_news[_dt_disp.fillna(pd.Timestamp(0)) >= cutoff_disp]

        st.caption(f"Showing **{len(filtered_news):,}** of **{len(df_news):,}** headlines".replace(',', '.'))
            
        st.dataframe(
            filtered_news,
            use_container_width=True,
            column_config={
                "url": st.column_config.LinkColumn("Read Original", display_text="Open Link \u2192"),
                "Date": st.column_config.TextColumn("Date"),
                "Pension Fund": st.column_config.TextColumn("Fund"),
                "Headline": st.column_config.TextColumn("Headline"),
            },
            hide_index=True
        )
    else:
        st.info("No news articles found in the database. Ensure the `scrape_fund_news.py` crawler has run.")
        
# ==========================================
# PAGE 8: GLOSSARY (BEGRIPPENLIJST)
# ==========================================
elif st.session_state.page == "Begrippenlijst":
    st.header("📖 Begrippenlijst (Glossary)")
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
