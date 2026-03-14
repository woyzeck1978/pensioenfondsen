import sqlite3
import pandas as pd
import os
import re
import fitz  # PyMuPDF

base_dir = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen"
db_path = os.path.join(base_dir, "data/processed/pension_funds.db")
pdf_dir = os.path.join(base_dir, "data/annual_reports")

def get_target_funds():
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("""
        SELECT id, name
        FROM funds 
        WHERE (equity_allocation_pct IS NULL OR aum_euro_bn IS NULL)
        AND status NOT LIKE '%Opgeheven%' 
        AND status NOT LIKE '%Liquidatie%' 
        AND status != 'Overdracht'
    """, conn)
    conn.close()
    return df

def match_pdf(fund_name, pdf_files):
    name_clean = fund_name.lower().replace('stichting', '').replace('pensioenfonds', '').strip()
    keywords = name_clean.split()
    
    # Priority 1: Exact keyword match
    for pdf in pdf_files:
        pdf_lower = pdf.lower()
        if all(kw in pdf_lower for kw in keywords if len(kw) > 2):
            return pdf
            
    # Priority 2: Partial match
    for pdf in pdf_files:
        pdf_lower = pdf.lower()
        if any(kw in pdf_lower for kw in keywords if len(kw) > 4):
            return pdf
            
    return None

def extract_aum_from_text(text):
    # Looking for phrases like: belegd vermogen 5,3 miljard, of 120 miljoen
    match = re.search(r'(?:belegd vermogen|vermogen van|pensioenvermogen|belegde vermogen)[\s:]*(?:van\s*)?(?:ruim\s*|ongeveer\s*|circa\s*)?€?\s*([\d,.]+)\s*(miljoen|miljard|mld|mln)', text, re.IGNORECASE)
    if match:
        val_str = match.group(1).replace(',', '.')
        try:
            val = float(val_str)
            unit = match.group(2).lower()
            if 'miljoen' in unit or 'mln' in unit:
                return round(val / 1000.0, 3) 
            return round(val, 3) # miljard
        except ValueError:
            pass
    return None

def extract_equity_from_text(text):
    # Looking for 'Aandelen 30%' or 'beleggingsmix... aandelen 30,5%' or 'Aandelen(fondsen) 30%'
    match = re.search(r'aandelen(?:portefeuille|fondsen)?[\s:]*[^\d]{0,40}([\d,.]+)\s*%', text, re.IGNORECASE)
    if match:
        val_str = match.group(1).replace(',', '.')
        try:
            val = float(val_str)
            if 0 < val <= 100:
                return val
        except ValueError:
            pass
    return None

def process_pdfs():
    df_missing = get_target_funds()
    pdf_files = [f for f in os.listdir(pdf_dir) if f.endswith('.pdf')]
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    aum_updates = 0
    equity_updates = 0
    
    print(f"Scanning local PDFs for {len(df_missing)} funds...")
    
    for idx, row in df_missing.iterrows():
        f_id = row['id']
        name = row['name']
        
        pdf_match = match_pdf(name, pdf_files)
        if not pdf_match:
            continue
            
        pdf_path = os.path.join(pdf_dir, pdf_match)
        try:
            doc = fitz.open(pdf_path)
            full_text = ""
            # Scan up to 100 pages to catch appendices and late financial tables
            for page_num in range(min(100, len(doc))):
                full_text += doc[page_num].get_text() + " "
            
            doc.close()
            
            aum = extract_aum_from_text(full_text)
            equity = extract_equity_from_text(full_text)
            
            if aum is not None:
                cursor.execute('UPDATE funds SET aum_euro_bn = ? WHERE id = ? AND aum_euro_bn IS NULL', (aum, f_id))
                aum_updates += cursor.rowcount
            
            if equity is not None:
                cursor.execute('UPDATE funds SET equity_allocation_pct = ? WHERE id = ? AND equity_allocation_pct IS NULL', (equity, f_id))
                equity_updates += cursor.rowcount
                
        except Exception as e:
            print(f"Error reading {pdf_match}: {e}")
            
    conn.commit()
    conn.close()
    
    print(f"Extraction complete! Found {aum_updates} missing AUMs and {equity_updates} missing Equity percentages from PDFs.")

if __name__ == '__main__':
    process_pdfs()
