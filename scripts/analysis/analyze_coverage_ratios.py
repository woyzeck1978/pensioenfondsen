import os
import sqlite3
import re
import fitz # PyMuPDF
from concurrent.futures import ProcessPoolExecutor, as_completed

DB_PATH = "../../data/processed/pension_funds.db"
REPORTS_DIR = "../data/reports"

def extract_text_from_pdf(pdf_path, max_pages=150):
    text = ""
    try:
        doc = fitz.open(pdf_path)
        for page_num in range(min(max_pages, len(doc))):
            text += doc.load_page(page_num).get_text() + " "
    except Exception:
        pass
    return text.lower().replace('\n', ' ')

def analyze_ratios(filename):
    pdf_path = os.path.join(REPORTS_DIR, filename)
    try:
        fund_id = int(filename.split('_')[0])
    except ValueError:
        return None

    text = extract_text_from_pdf(pdf_path)
    if not text: return None

    metrics = {
        'maanddekkingsgraad': None,
        'beleidsdekkingsgraad': None,
        'vereiste': None
    }

    # Helper function to convert Dutch formatted percentages "115,3%" or "115.3%" -> 115.3
    def parse_pct(s):
        s = s.replace('%', '').replace(',', '.').strip()
        try:
            val = float(s)
            # Coverage ratios are typically between 80% and 180%
            if 60 <= val <= 250:
                return val
        except ValueError:
            pass
        return None

    # We use a 200 character window to find the value usually residing to the right of the label in a table
    
    # 1. Maanddekkingsgraad / Actuele dekkingsgraad
    # Look for "actuele dekkingsgraad" or "maanddekkingsgraad" followed somewhat closely by a percentage
    m_maand = re.search(r'(?:maanddekkingsgraad|actuele dekkingsgraad|dekkingsgraad ultimo).{0,150}?(\d{2,3}(?:[.,]\d)?\s*%)', text)
    if m_maand: metrics['maanddekkingsgraad'] = parse_pct(m_maand.group(1))

    # 2. Beleidsdekkingsgraad
    m_beleid = re.search(r'beleidsdekkingsgraad.{0,150}?(\d{2,3}(?:[.,]\d)?\s*%)', text)
    if m_beleid: metrics['beleidsdekkingsgraad'] = parse_pct(m_beleid.group(1))
    
    # 3. Vereiste Dekkingsgraad
    m_vereist = re.search(r'vereiste\s+dekkingsgraad.{0,150}?(\d{2,3}(?:[.,]\d)?\s*%)', text)
    if m_vereist: metrics['vereiste'] = parse_pct(m_vereist.group(1))

    return (metrics['maanddekkingsgraad'], metrics['beleidsdekkingsgraad'], metrics['vereiste'], fund_id)

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    pdf_files = [f for f in os.listdir(REPORTS_DIR) if f.endswith('.pdf')]
    print(f"Scanning {len(pdf_files)} reports for coverage ratios...")

    results = []
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = {executor.submit(analyze_ratios, f): f for f in pdf_files}
        
        for i, future in enumerate(as_completed(futures), 1):
            res = future.result()
            if res:
                maand, beleid, vereiste, fund_id = res
                results.append((maand, beleid, vereiste, fund_id))
                print(f"[{i}/{len(pdf_files)}] ID {fund_id} -> Maand: {maand}%, Beleid: {beleid}%, Vereist: {vereiste}%")

    # The explicit database schema fields are: maanddekkingsgraad_pct, beleidsdekkingsgraad_pct, vereiste_dekkingsgraad_pct
    c.executemany("""
        UPDATE funds 
        SET maanddekkingsgraad_pct = ?, beleidsdekkingsgraad_pct = ?, vereiste_dekkingsgraad_pct = ? 
        WHERE id = ?
    """, results)
    
    conn.commit()
    conn.close()
    
    print(f"Done! {len(results)} metrics recorded.")

if __name__ == "__main__":
    main()
