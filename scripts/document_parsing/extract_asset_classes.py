import sqlite3
import pandas as pd
import fitz
import os
import re

base_dir = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen"
db_path = os.path.join(base_dir, "data/processed/pension_funds.db")
reports_dir = os.path.join(base_dir, "data/annual_reports")

def extract_allocations(text):
    results = {
        'fixed_income_pct': None,
        'real_estate_pct': None,
        'alternatives_pct': None
    }
    
    # Clean the text somewhat for easier regex
    text = text.replace('\\n', ' ')
    
    # Regular expressions for specific asset classes
    # Vastrentende waarden (Fixed Income)
    fixed_income_regex = re.compile(r'(?i)(?:vastrentend|obligatie)[^\d]{0,40}?(\d{1,2}(?:[.,]\d{1,2})?)\s*%')
    
    # Vastgoed (Real Estate)
    real_estate_regex = re.compile(r'(?i)(?:vastgoed|onroerend goed)[^\d]{0,40}?(\d{1,2}(?:[.,]\d{1,2})?)\s*%')
    
    # Alternatieven (Alternatives / Private Equity / Infrastructuur)
    alternatives_regex = re.compile(r'(?i)(?:alternatiev|private equity|infrastructuur|hedgefund|hedge fund)[^\d]{0,40}?(\d{1,2}(?:[.,]\d{1,2})?)\s*%')
    
    match_fi = fixed_income_regex.search(text)
    if match_fi:
        val = match_fi.group(1).replace(',', '.')
        try:
            results['fixed_income_pct'] = float(val)
        except ValueError: pass

    match_re = real_estate_regex.search(text)
    if match_re:
        val = match_re.group(1).replace(',', '.')
        try:
            results['real_estate_pct'] = float(val)
        except ValueError: pass

    match_alt = alternatives_regex.search(text)
    if match_alt:
        val = match_alt.group(1).replace(',', '.')
        try:
            results['alternatives_pct'] = float(val)
        except ValueError: pass
        
    return results

def get_target_funds():
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("""
        SELECT f.id, f.name
        FROM funds f
        WHERE f.annual_report_downloaded = 1
    """, conn)
    conn.close()
    return df

def process_pdf(pdf_path):
    allocations = {
        'fixed_income_pct': None,
        'real_estate_pct': None,
        'alternatives_pct': None
    }
    
    try:
        doc = fitz.open(pdf_path)
        
        # We search specifically for pages containing keywords like "beleggingsmix", "feitelijke weging", "strategisch"
        focus_keywords = ['beleggingsmix', 'beleggingsportefeuille', 'actuele weging', 'strategische weging', 'feitelijke weging', 'vermogensverdeling']
        
        full_text = ""
        for page_num in range(doc.page_count):
            page = doc.load_page(page_num)
            text = page.get_text("text")
            
            text_lower = text.lower()
            if any(kw in text_lower for kw in focus_keywords):
                full_text += text + " "
                
        if full_text:
            allocations = extract_allocations(full_text)
            
        doc.close()
    except Exception as e:
        print(f"Error reading PDF {pdf_path}: {e}")
        
    return allocations

def main():
    df = get_target_funds()
    total = len(df)
    updates = []
    
    print(f"Scanning {total} Annual Report PDFs for missing Portfolio Allocations...")
    
    for index, row in df.iterrows():
        f_id = row['id']
        name = row['name']
        
        pdf_path = None
        for filename in os.listdir(reports_dir):
            if filename.startswith(f"{f_id}_") and filename.endswith(".pdf"):
                pdf_path = os.path.join(reports_dir, filename)
                break
        
        if pdf_path:
            # print(f"Processing: {name}")
            data = process_pdf(pdf_path)
            
            # If we found at least one of them
            if any(v is not None for v in data.values()):
                updates.append({
                    'id': f_id,
                    'fixed_income_pct': data['fixed_income_pct'],
                    'real_estate_pct': data['real_estate_pct'],
                    'alternatives_pct': data['alternatives_pct']
                })
        else:
             print(f"File not found for ID: {f_id} - {name}")
             
    if updates:
        print(f"\\nFound allocations for {len(updates)} funds. Updating database...")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        for u in updates:
            # We use COALESCE so we don't accidentally overwrite existing data with NULLs if we only found a partial match
            cursor.execute('''
                UPDATE funds 
                SET fixed_income_pct = COALESCE(?, fixed_income_pct),
                    real_estate_pct = COALESCE(?, real_estate_pct),
                    alternatives_pct = COALESCE(?, alternatives_pct)
                WHERE id = ?
            ''', (u['fixed_income_pct'], u['real_estate_pct'], u['alternatives_pct'], u['id']))
            
        conn.commit()
        conn.close()
        print(f"Successfully updated database!")
    else:
        print("No new allocations found to save.")

if __name__ == '__main__':
    main()
