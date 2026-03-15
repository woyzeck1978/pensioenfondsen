import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import os
import urllib.parse

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Dutch Pension Funds Explorer",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

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
        f.id, f.name, f.category, f.aum_euro_bn, f.dekkingsgraad_pct, 
        f.equity_allocation_pct, f.uitvoerder, f.deelnemers_totaal, f.website,
        f.deelnemers_actief, f.deelnemers_slapers, f.deelnemers_gepensioneerd,
        f.sfdr_article, f.eu_taxonomy_pct,
        e.co2_reduction_goal, e.sfdr_classification
    FROM funds f
    LEFT JOIN fund_esg_metrics e ON f.id = e.fund_id
    """
    return load_data(query)

@st.cache_data(ttl=3600)
def get_latest_news():
    query = """
    SELECT 
        n.published_date as "Date", 
        f.name as "Pension Fund", 
        n.title as "Headline", 
        n.url, 
        f.category as "Category"
    FROM news_articles n
    JOIN funds f ON n.fund_id = f.id
    ORDER BY n.published_date DESC
    LIMIT 300
    """
    return load_data(query)

def get_metrics_history(fund_id):
    query = f"""
    SELECT year, economische_dekkingsgraad_pct, nominale_dekkingsgraad_pct, 
           beleidsdekkingsgraad_pct, reele_dekkingsgraad_pct, 
           beleggingsrendement_pct, indexatieverlening_pct, cpi_pct
    FROM historical_metrics
    WHERE fund_id = {fund_id}
    ORDER BY year ASC
    """
    return load_data(query)

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
    query = f"""
    SELECT year_extracted, title, url
    FROM (
        SELECT 
            title, 
            url,
            CAST(SUBSTR(title, -4) AS INTEGER) as year_extracted
        FROM scraped_documents
        WHERE fund_id = {fund_id} 
          AND doc_type = 'document' 
          AND (lower(title) LIKE '%jaarverslag%' OR lower(title) LIKE '%jaarrapport%' OR lower(title) LIKE '%annual report%')
          AND lower(title) NOT LIKE '%maatschappelijk%'
          AND lower(title) NOT LIKE '%duurzaam%'
          AND lower(title) NOT LIKE '%esg%'
    )
    ORDER BY year_extracted DESC NULLS LAST, title DESC
    LIMIT 5
    """
    return load_data(query)

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

pages = ["Sector Overview", "Fund Deep-Dive", "Asset Managers Exposure", "WTP Tracker", "Dekkingsgraad Analysis", "ESG & SFDR Tracker", "Industry News Feed"]

# Sidebar Navigation
st.sidebar.title("Navigation")
# Use the session_state 'page' as the key to bind it bidirectionally
st.sidebar.radio("Go to", pages, key="page")

st.sidebar.markdown("---")
st.sidebar.info(f"**Database Stats:**\\nTracking **{len(df_funds)}** Pension Funds.\\nTotal AUM: **€{df_funds['aum_euro_bn'].sum():,.1f} Billion**")


# ==========================================
# PAGE 1: SECTOR OVERVIEW
# ==========================================
if st.session_state.page == "Sector Overview":
    st.header("Sector Overview")
    
    # KPI Row
    col1, col2, col3, col4 = st.columns(4)
    valid_aum = df_funds.dropna(subset=['aum_euro_bn'])
    valid_ratio = df_funds.dropna(subset=['dekkingsgraad_pct'])
    
    col1.metric("Total AUM Tracked", f"€{valid_aum['aum_euro_bn'].sum():,.1f} Bn")
    col2.metric("Average Funding Ratio", f"{valid_ratio['dekkingsgraad_pct'].mean():.1f}%")
    col3.metric("Largest Fund", valid_aum.loc[valid_aum['aum_euro_bn'].idxmax()]['name'])
    col4.metric("Funds Tracked", len(df_funds))
    
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
            labels={"dekkingsgraad_pct": "Funding Ratio (%)", "aum_euro_bn": "AUM (Billion €)"},
            log_y=True, # Log scale because ABP/PFZW skew the Y axis massively
            title="Log(AUM) vs Funding Ratio"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
        
    with c2:
        st.subheader("Market Share by Category")
        market_share = df_funds.groupby('category')['aum_euro_bn'].sum().reset_index()
        fig_pie = px.pie(market_share, values='aum_euro_bn', names='category', title="Total AUM Distribution")
        st.plotly_chart(fig_pie, use_container_width=True)
        
    st.divider()
    st.subheader("Fund Directory")
    st.markdown("💡 *Click the **View Profile** link to jump directly into the fund's specific Deep-Dive!*")
    
    # Create the display dataframe
    df_display = df_funds[['name', 'category', 'aum_euro_bn', 'dekkingsgraad_pct', 'equity_allocation_pct', 'uitvoerder']].copy()
    
    # Inject an explicit URL mapping column to support Streamlit versions < 1.35
    df_display.insert(0, 'Profile Link', df_display['name'].apply(lambda x: f"/?fund={urllib.parse.quote_plus(x)}"))
    
    st.dataframe(
        df_display, 
        use_container_width=True,
        column_config={
            "Profile Link": st.column_config.LinkColumn(
                "Action",
                help="Click to view the Deep-Dive profile.",
                display_text="View Profile \u2192"
            )
        }
    )

# ==========================================
# PAGE 2: FUND DEEP-DIVE
# ==========================================
elif st.session_state.page == "Fund Deep-Dive":
    st.header("Fund Profile Deep-Dive")
    
    # Fund Selector
    fund_names = df_funds['name'].sort_values().tolist()
    
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
        if pd.notnull(fund_data['website']) and fund_data['website'] != "":
            st.markdown(f"🌐 **Website:** [{fund_data['website']}]({fund_data['website']})")
        
        # Top KPIs
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric("AUM", f"€{fund_data['aum_euro_bn']} Bn" if pd.notnull(fund_data['aum_euro_bn']) else "N/A")
        kpi2.metric("Funding Ratio", f"{fund_data['dekkingsgraad_pct']}%" if pd.notnull(fund_data['dekkingsgraad_pct']) else "N/A")
        kpi3.metric("Equity Allocation", f"{fund_data['equity_allocation_pct']}%" if pd.notnull(fund_data['equity_allocation_pct']) else "N/A")
        kpi4.metric("Participants", f"{fund_data['deelnemers_totaal']:,.0f}" if pd.notnull(fund_data['deelnemers_totaal']) else "N/A")
        
        st.divider()
        
        col_main, col_side = st.columns([2, 1])
        
        with col_main:
            st.markdown("### Historical Performance")
            history_df = get_metrics_history(fund_id)
            if not history_df.empty:
                fig_line = px.line(history_df, x="year", y=["beleidsdekkingsgraad_pct", "beleggingsrendement_pct"], 
                                   labels={"value": "Percentage (%)", "year": "Year", "variable": "Metric"},
                                   title="Funding Ratio & Investment Return History")
                fig_line.update_xaxes(dtick=1, tickformat="d")
                st.plotly_chart(fig_line, use_container_width=True)
                
                st.markdown("#### Meerjarenoverzicht (Multi-Year Overview)")
                
                rename_map = {
                    'economische_dekkingsgraad_pct': 'Actuele dekkingsgraad',
                    'nominale_dekkingsgraad_pct': 'Nominale dekkingsgraad',
                    'beleidsdekkingsgraad_pct': 'Beleidsdekkingsgraad',
                    'reele_dekkingsgraad_pct': 'Reële dekkingsgraad',
                    'beleggingsrendement_pct': 'Totaal rendement',
                    'indexatieverlening_pct': 'Indexatie (toeslag)',
                    'cpi_pct': 'CPI (Prijsinflatie)'
                }
                
                table_df = history_df.rename(columns=rename_map)
                
                # Group by year to handle any duplicate database entries for the same year
                table_df = table_df.groupby('year').last().T
                table_df = table_df[sorted(table_df.columns, reverse=True)]
                
                for col in table_df.columns:
                    table_df[col] = table_df[col].apply(lambda x: f"{x:.1f}%" if pd.notnull(x) else "-")
                    
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
            article = str(int(fund_data['sfdr_article'])) if pd.notnull(fund_data['sfdr_article']) else "Not extracted"
            tax_pct = f"{fund_data['eu_taxonomy_pct']}%" if pd.notnull(fund_data['eu_taxonomy_pct']) else "Not extracted"
            
            st.markdown(f"**Extracted SFDR Article (2024):** {article}")
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
    
    if not wtp_df.empty:
        c1, c2 = st.columns(2)
        
        with c1:
            timeline_counts = wtp_df['wtp_transitie_datum'].value_counts().reset_index()
            timeline_counts.columns = ['Transition Date', 'Number of Funds']
            timeline_counts = timeline_counts.sort_values('Transition Date')
            fig_bar = px.bar(timeline_counts, x='Transition Date', y='Number of Funds', title="Funds Transitioning per Date")
            st.plotly_chart(fig_bar, use_container_width=True)
            
        with c2:
            contract_counts = wtp_df['wtp_contract_type'].value_counts().reset_index()
            contract_counts.columns = ['Contract Type', 'Count']
            fig_pie = px.pie(contract_counts, names='Contract Type', values='Count', title="Planned Contract Types (SPR vs FPR)")
            st.plotly_chart(fig_pie, use_container_width=True)
            
        st.dataframe(wtp_df, use_container_width=True)
    else:
        st.info("No WTP transition data available in the database yet.")

# ==========================================
# PAGE 5: DEKKINGSGRAAD ANALYSIS
# ==========================================
elif st.session_state.page == "Dekkingsgraad Analysis":
    st.header("Funding Ratios (Dekkingsgraad)")
    st.markdown("Analysis of the financial health and funding ratios of Dutch pension funds.")
    
    valid_df = df_funds.dropna(subset=['dekkingsgraad_pct']).copy()
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
        c1, c2 = st.columns(2)
        with c1:
            article_counts = sfdr_df['sfdr_article'].value_counts().reset_index()
            article_counts.columns = ['SFDR Article', 'Fund Count']
            article_counts['SFDR Article'] = 'Article ' + article_counts['SFDR Article'].astype(int).astype(str)
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
        c1, c2 = st.columns([1, 2])
        with c1:
            cat_filter = st.multiselect("Filter by Fund Category:", df_news["Category"].dropna().unique())
        with c2:
            search_query = st.text_input("Search Headlines (e.g., 'indexatie', 'premie'):")
            
        filtered_news = df_news.copy()
        if cat_filter:
            filtered_news = filtered_news[filtered_news["Category"].isin(cat_filter)]
        if search_query:
            filtered_news = filtered_news[filtered_news["Headline"].str.contains(search_query, case=False, na=False)]
            
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
