import sqlite3
import pandas as pd
import pdfplumber
import fitz
import os
import re

base_dir = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen"
db_path = os.path.join(base_dir, "data/processed/pension_funds.db")
reports_dir = os.path.join(base_dir, "data/annual_reports")

def clean_dutch_number(val_str):
    if '.' in val_str and ',' in val_str:
        if val_str.rfind(',') > val_str.rfind('.'):
            val_str = val_str.replace('.', '').replace(',', '.')
        else:
            val_str = val_str.replace(',', '')
    elif ',' in val_str:
        if len(val_str) - val_str.rfind(',') == 4:
            val_str = val_str.replace(',', '')
        else:
            val_str = val_str.replace(',', '.')
    elif '.' in val_str:
         if len(val_str) - val_str.rfind('.') == 4:
            val_str = val_str.replace('.', '')
    try:
        return float(val_str)
    except:
        return None

def normalize_to_millions(val_raw):
    if val_raw > 1000000:
        return round(val_raw / 1000000, 2)
    elif val_raw > 1000:
        return round(val_raw / 1000, 2)
    else:
        return val_raw

def extract_fee_from_text(text):
    number_regex = re.compile(r'(?i)(?:performance\s*fee|prestatievergoeding|prestatiebeloning)[^\d]{0,35}?(\d{1,4}(?:[.,]\d{3})*(?:[.,]\d+)?)\b')
    match = number_regex.search(text)
    if match:
        val_raw = clean_dutch_number(match.group(1))
        if val_raw is not None:
            return normalize_to_millions(val_raw)
    return None

def process_pdf(pdf_path):
    fee = None
    focus_keywords = ['prestatievergoeding', 'prestatiebeloning', 'performance fee', 'vermogensbeheerkosten', 'uitvoeringskosten']
    
    # PASS 1: Fast Regex on pure text via fitz
    try:
        doc = fitz.open(pdf_path)
        for page_num in range(doc.page_count):
            page = doc.load_page(page_num)
            text = page.get_text("text")
            if any(kw in text.lower() for kw in focus_keywords):
                clean_t = text.replace('\n', ' ')
                res = extract_fee_from_text(clean_t)
                if res is not None:
                    fee = res
                    break
        doc.close()
    except: pass
    
    # PASS 2: Table extraction via pdfplumber for the stubborn ones
    if fee is None:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                # To speed things up, only check first 100 pages
                for page in pdf.pages[:100]:
                    text = page.extract_text() or ""
                    if "performance fee" in text.lower() or "prestatievergoeding" in text.lower():
                        tables = page.extract_tables()
                        for table in tables:
                            for row in table:
                                row_text = " ".join([str(cell).lower() for cell in row if cell])
                                if 'performance fee' in row_text or 'prestatievergoeding' in row_text:
                                    # Look for the first number in the row
                                    numbers = re.findall(r'(\d{1,4}(?:[.,]\d{3})*(?:[.,]\d+)?)\b', row_text)
                                    if numbers:
                                        val_raw = clean_dutch_number(numbers[-1]) # Usually the last column is the current year
                                        if val_raw:
                                            fee = normalize_to_millions(val_raw)
                                            break
                            if fee: break
                    if fee: break
        except Exception as e:
            pass
            
    return fee

def main():
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT id, name FROM funds WHERE annual_report_downloaded = 1", conn)
    conn.close()
    
    updates = []
    print(f"Scanning {len(df)} Annual Report PDFs for Performance Fees...")
    
    for _, row in df.iterrows():
        f_id, name = row['id'], row['name']
        pdf_path = next((os.path.join(reports_dir, f) for f in os.listdir(reports_dir) if f.startswith(f"{f_id}_") and f.endswith(".pdf")), None)
        
        if pdf_path:
            fee = process_pdf(pdf_path)
            if fee is not None:
                updates.append({'id': f_id, 'performance_fee_miljoenen': fee})
             
    if updates:
        print(f"\nFound Performance Fees for {len(updates)} funds. Updating database...")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        for u in updates:
            cursor.execute('UPDATE funds SET performance_fee_miljoenen = ? WHERE id = ?', (u['performance_fee_miljoenen'], u['id']))
        conn.commit()
        conn.close()
    else:
        print("No Performance Fees found.")

if __name__ == '__main__':
    main()
