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

def extract_strategy_notes(text):
    if not text:
        return None
        
    paragraphs = text.split('\n')
    relevant_paragraphs = []
    
    # We want paragraphs that sound definitive about equity strategy
    target_keywords = ['aandelen', 'beleggingsbeleid', 'aandelenportefeuille', 'passief', 'actief']
    esg_keywords = ['duurzaam', 'esg', 'klimaat', 'uitsluitingen']
    
    for p in paragraphs:
        p_lower = p.lower()
        
        # Look for substantive paragraphs, not just single words or table headers
        if len(p_lower) < 80:
            continue
            
        has_target = any(k in p_lower for k in target_keywords)
        has_esg = any(k in p_lower for k in esg_keywords)
        has_strategic_flavor = ('beleid' in p_lower or 'strategie' in p_lower or 'allocatie' in p_lower or 'portefeuille' in p_lower)
        
        if has_target and has_strategic_flavor:
            relevant_paragraphs.append(p.strip())
            
    # We don't want to dump too much text. Let's pick the 3 longest distinct paragraphs.
    if not relevant_paragraphs:
        return None
        
    relevant_paragraphs.sort(key=len, reverse=True)
    best_pics = relevant_paragraphs[:3]
    
    notes = " | ".join(best_pics)
    
    # Truncate to avoid exploding the DB, but keep the meat of the strategy
    return notes[:1500]

def analyze_equity_strategy(filename):
    pdf_path = os.path.join(REPORTS_DIR, filename)
    try:
        fund_name_from_file = filename.replace('.pdf', '')
        fund_id = int(fund_name_from_file.split('_')[0])
    except ValueError:
        return None

    text = extract_text_from_pdf(pdf_path)
    if not text: 
        return None

    notes = extract_strategy_notes(text)
    
    # If we also want to fish for the % here we can, but let's just focus on the notes as we did a quick pass for %
    # However we can try to extract allocation % from 'Aandelen' if missing from the text using similar regex.
    alloc_pct = None
    pct_matches = re.findall(r'(\d{1,2}(?:[.,]\d{1,2})?)\s*%\s*(?:in\s+)?aandelen', text.lower())
    if pct_matches:
        try:
            alloc_pct = float(pct_matches[0].replace(',', '.'))
        except ValueError:
            pass

    return fund_id, notes, alloc_pct

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Funds missing notes
    c.execute("SELECT id, name FROM funds WHERE equity_strategy_notes IS NULL OR equity_strategy_notes = ''")
    target_funds = {str(row[0]) for row in c.fetchall()}
    
    print(f"Funds needing equity strategy notes: {len(target_funds)}")
    
    pdf_files = []
    for f in os.listdir(REPORTS_DIR):
        if f.endswith('.pdf'):
            fid = f.split('_')[0]
            if fid in target_funds:
                pdf_files.append(f)
                
    print(f"Found {len(pdf_files)} reports to analyze.")

    updates = []
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = {executor.submit(analyze_equity_strategy, f): f for f in pdf_files}
        
        for i, future in enumerate(as_completed(futures), 1):
            filename = futures[future]
            try:
                res = future.result()
                if res is not None:
                    fund_id, notes, alloc_pct = res
                    if notes:
                        updates.append((notes, fund_id))
                        print(f"[{i}/{len(pdf_files)}] {filename} -> FOUND NOTES")
            except Exception as e:
                print(f"Error processing {filename}: {e}")

    if updates:
        c.executemany("UPDATE funds SET equity_strategy_notes = ? WHERE id = ?", updates)
        conn.commit()
    conn.close()
    
    print(f"Updated notes for {len(updates)} funds.")

if __name__ == "__main__":
    main()
