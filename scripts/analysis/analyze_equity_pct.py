import os
import sqlite3
import re
import fitz # PyMuPDF
from concurrent.futures import ProcessPoolExecutor, as_completed

DB_PATH = "../../data/processed/pension_funds.db"
REPORTS_DIR = "data/reports"

def extract_text_from_pdf(pdf_path, max_pages=150):
    text = ""
    try:
        doc = fitz.open(pdf_path)
        for page_num in range(min(max_pages, len(doc))):
            text += doc.load_page(page_num).get_text() + " \n"
    except Exception as e:
        pass
    return text

def analyze_equity_pct(filename):
    pdf_path = os.path.join(REPORTS_DIR, filename)
    try:
        fund_name_from_file = filename.replace('.pdf', '')
        fund_id = int(fund_name_from_file.split('_')[0])
    except ValueError:
        return None

    text = extract_text_from_pdf(pdf_path)
    if not text: 
        return None

    alloc_pct = None
    
    patterns = [
        r'(\d{1,2}(?:[.,]\d{1,2})?)\s*%\s*(?:in\s+)?(?:aandelen|zakelijke waarden)',
        r'(?:aandelen|zakelijke waarden)[:\s]+(\d{1,2}(?:[.,]\d{1,2})?)\s*%',
        r'aandelenportefeuille\s+van\s+(\d{1,2}(?:[.,]\d{1,2})?)\s*%',
        r'strategische allocatie[^\d]{0,50}?aandelen[^\d]{0,20}?(\d{1,2}(?:[.,]\d{1,2})?)\s*%'
    ]
    
    text_lower = text.lower()
    
    best_pct = None
    for pattern in patterns:
        matches = re.finditer(pattern, text_lower)
        for m in matches:
            val = float(m.group(1).replace(',', '.'))
            if 5 <= val <= 95:  # realistic equity bounds
                best_pct = val
                break
        if best_pct is not None:
            break
            
    # Table parsing fallback
    if best_pct is None:
        table_matches = re.findall(r'aandelen[^\d\n%]{0,30}(\d{1,2}(?:[.,]\d{1,2})?)\s*%', text_lower)
        for match in table_matches:
            val = float(match.replace(',', '.'))
            if 5 <= val <= 95:
                best_pct = val
                break

    return fund_id, best_pct

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT id FROM funds WHERE equity_allocation_pct IS NULL")
    target_funds = {str(row[0]) for row in c.fetchall()}
    
    print(f"Funds needing equity allocation pct: {len(target_funds)}")
    
    pdf_files = []
    if os.path.exists(REPORTS_DIR):
        for f in os.listdir(REPORTS_DIR):
            if f.endswith('.pdf'):
                fid = f.split('_')[0]
                if fid in target_funds:
                    pdf_files.append(f)
                    
    print(f"Found {len(pdf_files)} reports to analyze.")

    updates = []
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = {executor.submit(analyze_equity_pct, f): f for f in pdf_files}
        
        for i, future in enumerate(as_completed(futures), 1):
            filename = futures[future]
            try:
                res = future.result()
                if res is not None:
                    fund_id, alloc_pct = res
                    if alloc_pct is not None:
                        updates.append((alloc_pct, fund_id))
                        print(f"[{i}/{len(pdf_files)}] {filename} -> FOUND ALLOC PCT: {alloc_pct}%")
            except Exception as e:
                pass

    if updates:
        c.executemany("UPDATE funds SET equity_allocation_pct = ? WHERE id = ?", updates)
        conn.commit()
    conn.close()
    
    print(f"Updated equity_allocation_pct for {len(updates)} funds.")

if __name__ == "__main__":
    main()
