import pandas as pd
import sqlite3
import os
import re
from difflib import SequenceMatcher

base_dir = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen"
wtp_path = os.path.join(base_dir, "data/raw/Overzicht pensioentransitie wtp.xlsx")
db_path = os.path.join(base_dir, "data/processed/pension_funds.db")
output_path = os.path.join(base_dir, "data/processed/Overzicht pensioentransitie_improved.xlsx")

def similar(a, b):
    return SequenceMatcher(None, str(a).lower(), str(b).lower()).ratio()

def generate_improved_wtp():
    # 1. Read the original WTP file
    # The header seems to span multiple lines. Looking at the output, row index 4 (0-indexed 3) has the actual sub-headers.
    # Let's read it keeping the formatting as best as we can, or we extract the data and rebuild it neatly.
    df_wtp = pd.read_excel(wtp_path, header=None) # Read entirely raw
    
    # We want to preserve the first 4 rows which seem to contain titles and merged headers
    # The actual data starts at row index 5
    
    # Let's clean it up to make a truly "improved" version: a flat, highly readable table
    # Row index 3 has some headers, row index 4 has some headers. 
    headers_row_1 = df_wtp.iloc[3].fillna('').astype(str).tolist()
    headers_row_2 = df_wtp.iloc[4].fillna('').astype(str).tolist()
    
    # Combine headers if needed
    final_headers = []
    for h1, h2 in zip(headers_row_1, headers_row_2):
        if h2.strip():
            final_headers.append(h2.strip())
        else:
            final_headers.append(h1.strip())
            
    # Re-read with correct headers
    df = pd.read_excel(wtp_path, skiprows=4)
    # The columns might be slightly messed up because of merged cells in excel. Let's manually set clean ones.
    # From observation: 
    # ['Type', 'Naam fonds', '2025', '2026', '2027', '2028', ...]
    
    df.columns = final_headers
    
    # Clean up empty rows
    df = df.dropna(subset=[final_headers[2]]) # Assuming index 2 is Naam fonds
    
    # Rename columns for clarity if needed
    col_mapping = {}
    for i, col in enumerate(df.columns):
        if col.startswith('Unnamed'):
            col_mapping[col] = f'Data_Col_{i}'
        elif col == '':
            col_mapping[col] = f'Data_Col_{i}'
            
    df = df.rename(columns=col_mapping)
    
    # Fund name column
    fund_col = [c for c in df.columns if 'naam' in c.lower() or 'fonds' in c.lower()][0]

    # 2. Get data from database
    conn = sqlite3.connect(db_path)
    # Fetch exactly what we want to append
    query = """
    SELECT 
        name as db_name, 
        aum_euro_bn as db_aum_bn,
        dekkingsgraad_pct as db_dekkingsgraad,
        equity_allocation_pct as db_equity_pct,
        equity_strategy_notes as db_equity_strategy,
        website as db_website
    FROM funds
    """
    df_db = pd.read_sql_query(query, conn)
    conn.close()

    # 3. Fuzzy Merge
    # We will try to match the WTP fund name to the DB fund name
    # Exact match first, then fuzzy
    def get_best_match(wtp_name):
        wtp_name = str(wtp_name).strip()
        
        # exact
        exact = df_db[df_db['db_name'].str.lower() == wtp_name.lower()]
        if not exact.empty:
            return exact.iloc[0].to_dict()
            
        # check if wtp_name is substring of db_name
        substr1 = df_db[df_db['db_name'].str.lower().str.contains(wtp_name.lower(), regex=False, na=False)]
        if not substr1.empty:
            return substr1.iloc[0].to_dict()
            
        # check if db_name is substring of wtp_name
        for idx, row in df_db.iterrows():
            if str(row['db_name']).lower() in wtp_name.lower():
                return row.to_dict()
                
        # fuzzy
        best_score = 0
        best_row = None
        for idx, row in df_db.iterrows():
            db_name = str(row['db_name'])
            # Explicit Aliases for KLM
            if "KLM Algemeen (Ground Staff)" in wtp_name or "KLM Grondpersoneel" in wtp_name:
                if "KLM" in db_name and "Algemeen" in db_name: return row.to_dict()
                if "KLM" in db_name and "Grond" in db_name: return row.to_dict()
            elif "KLM Cabinepersoneel" in wtp_name or "(Cabin Crew)" in wtp_name:
                if "KLM" in db_name and "Cabine" in db_name: return row.to_dict()
            elif "KLM Vliegend" in wtp_name or "(Flight Crew)" in wtp_name:
                if "KLM" in db_name and "Vliegend" in db_name: return row.to_dict()
            elif "Rail & OV" in wtp_name:
                if "Rail" in db_name and "Openbaar" in db_name: return row.to_dict()
            elif "Banden en wielen" in wtp_name:
                if "Banden" in db_name and "Wielen" in db_name: return row.to_dict()

            score = similar(wtp_name, db_name)
            if score > best_score:
                best_score = score
                best_row = row
        
        if best_score > 0.65: # Threshold
            return best_row.to_dict()
            
        return {
            'db_name': None,
            'db_aum_bn': None,
            'db_dekkingsgraad': None,
            'db_equity_pct': None,
            'db_equity_strategy': None,
            'db_website': None
        }

    # Apply match
    merged_data = []
    for idx, row in df.iterrows():
        match = get_best_match(row[fund_col])
        combined = {**row.to_dict(), **match}
        merged_data.append(combined)

    df_final = pd.DataFrame(merged_data)
    
    # Reorder columns to put our enriched DB columns right after the fund name
    cols = list(df_final.columns)
    db_cols = ['db_name', 'db_aum_bn', 'db_dekkingsgraad', 'db_equity_pct', 'db_equity_strategy', 'db_website']
    for c in db_cols:
        if c in cols: cols.remove(c)
        
    fund_col_idx = cols.index(fund_col)
    
    # Insert new DB columns after the WTP fund name
    for i, c in enumerate(db_cols):
        cols.insert(fund_col_idx + 1 + i, c)
        
    df_final = df_final[cols]

    # Clean up column names for final excel
    rename_mapping = {
        'db_name': 'Matched DB Name (Check)',
        'db_aum_bn': 'AUM (€ Bn)',
        'db_dekkingsgraad': 'Dekkingsgraad (%)',
        'db_equity_pct': 'Equity Allocation (%)',
        'db_equity_strategy': 'Equity Strategy Notes',
        'db_website': 'Website'
    }
    df_final = df_final.rename(columns=rename_mapping)

    # Export to Excel with formatting
    writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
    df_final.to_excel(writer, index=False, sheet_name='WTP_Enriched')
    
    workbook = writer.book
    worksheet = writer.sheets['WTP_Enriched']
    
    header_format = workbook.add_format({
        'bold': True,
        'bg_color': '#D9E1F2',
        'border': 1
    })
    
    # Apply header format
    for col_num, value in enumerate(df_final.columns.values):
        worksheet.write(0, col_num, value, header_format)
        
    # Auto-adjust column widths
    for i, col in enumerate(df_final.columns):
        if col == 'Equity Strategy Notes':
            worksheet.set_column(i, i, 80) # Very wide for notes
        elif col in ['Naam fonds', 'Matched DB Name (Check)', 'Website']:
            worksheet.set_column(i, i, 40)
        else:
            column_len = max(df_final[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, min(column_len, 25))

    worksheet.freeze_panes(1, 0) # Freeze top row
    worksheet.autofilter(0, 0, len(df_final), len(df_final.columns) - 1)
    
    writer.close()
    print(f"Successfully generated enriched WTP Excel at: {output_path}")

if __name__ == '__main__':
    generate_improved_wtp()
