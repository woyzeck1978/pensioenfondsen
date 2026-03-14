import sqlite3
import pandas as pd
import xlsxwriter

def export_to_excel(db_path, excel_path):
    conn = sqlite3.connect(db_path)
    
    # Get all tables
    tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
    tables = pd.read_sql_query(tables_query, conn)['name'].tolist()
    
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(excel_path, engine='xlsxwriter')
    
    for table_name in tables:
        if table_name == 'funds':
            df = pd.read_sql_query(f'SELECT * FROM {table_name} ORDER BY category ASC, aum_euro_bn DESC', conn)
        elif table_name == 'historical_metrics':
            df = pd.read_sql_query(f'SELECT f.name as fund_name, h.* FROM historical_metrics h JOIN funds f ON h.fund_id = f.id ORDER BY h.fund_id, h.year DESC', conn)
        elif table_name == 'scraped_documents':
            df = pd.read_sql_query(f'SELECT f.name as fund_name, s.doc_type, s.title, s.url, date(s.discovered_at) as discovered_date FROM scraped_documents s JOIN funds f ON s.fund_id = f.id ORDER BY f.name ASC, s.doc_type ASC', conn)
        elif table_name == 'news_articles':
            df = pd.read_sql_query(f'SELECT f.name as fund_name, n.published_date, n.title, n.url, n.content FROM news_articles n JOIN funds f ON n.fund_id = f.id ORDER BY f.name ASC, n.published_date DESC', conn)
        elif table_name == 'equity_portfolio_funds':
            df = pd.read_sql_query(f'SELECT f.name as pension_fund, e.fund_name as asset_manager FROM equity_portfolio_funds e JOIN funds f ON e.fund_id = f.id ORDER BY f.name ASC, e.fund_name ASC', conn)
        elif table_name == 'fund_esg_metrics':
            df = pd.read_sql_query(f'SELECT f.name as fund_name, e.sfdr_classification, e.esg_exclusions, e.co2_reduction_goal FROM fund_esg_metrics e JOIN funds f ON e.fund_id = f.id ORDER BY f.name ASC', conn)
        else:
            df = pd.read_sql_query(f'SELECT * FROM {table_name}', conn)
        
        # Explicit column reordering for the 'funds' table
        if table_name == 'funds':
            cols = list(df.columns)
            
            # Position Management specifics after the website
            for col in ['uitvoerder', 'fiduciair_beheerder', 'intern_beheer']:
                if col in cols:
                    cols.remove(col)
                    website_idx = cols.index('website') if 'website' in cols else 2
                    cols.insert(website_idx + 1, col)
            
            if 'dekkingsgraad_pct' in cols and 'aum_euro_bn' in cols:
                cols.remove('dekkingsgraad_pct')
                aum_idx = cols.index('aum_euro_bn')
                cols.insert(aum_idx + 1, 'dekkingsgraad_pct')
            if 'annual_report_year' in cols and 'annual_report_downloaded' in cols:
                cols.remove('annual_report_year')
                download_idx = cols.index('annual_report_downloaded')
                cols.insert(download_idx + 1, 'annual_report_year')
            df = df[cols]
                
        df.to_excel(writer, sheet_name=table_name, index=False)
        
        # Get the xlsxwriter workbook and worksheet objects
        workbook  = writer.book
        worksheet = writer.sheets[table_name]
        
        # Add a format for the hyperlink
        url_format = workbook.add_format({
            'font_color': 'blue',
            'underline':  True
        })
        
        # Format dataset
        for url_col in ['website', 'url', 'source_url']:
            if url_col in df.columns:
                col_idx = df.columns.get_loc(url_col)
                worksheet.set_column(col_idx, col_idx, 60) # Widen the column
                
                # Rewrite the URLs as proper hyperlinks
                for row_idx, url_val in enumerate(df[url_col]):
                    if pd.notna(url_val):
                        # +1 because Excel rows are 1-indexed, and +1 again to skip the header row
                        worksheet.write_url(row_idx + 1, col_idx, str(url_val), url_format, str(url_val))
                        
        # Optional: auto-adjust column widths for readability based on header length
        for i, col in enumerate(df.columns):
            if col not in ['website', 'url']: # we already set this one
                column_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(i, i, min(column_len, 50)) # cap at 50 width
                
        # Freeze the first row
        worksheet.freeze_panes(1, 0)
        
        # Add an autofilter to all columns and rows
        worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)

    writer.close()
    conn.close()
    print(f"Successfully exported {len(tables)} tables to {excel_path} with clickable hyperlinks.")

if __name__ == "__main__":
    db = '../../data/processed/pension_funds.db'
    out = '../../data/processed/pension_funds.xlsx'
    export_to_excel(db, out)
