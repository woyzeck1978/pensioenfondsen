import sqlite3
import os
import fitz
import re

DB_PATH = "../../data/processed/pension_funds.db"
REPORTS_DIR = "../data/reports"

FUNDS_TO_CHECK = [
    77, 81, 88, 92, 93, 94, 99, 103, 114, 115, 120, 122, 125, 126, 127, 128
]

def extract_aum_from_pdf(pdf_path):
    try:
        doc = fitz.open(pdf_path)
        text = ""
        # Check first 50 pages for the balance sheet / key figures
        for page_num in range(min(50, len(doc))):
            text += doc.load_page(page_num).get_text() + " "
        
        text = text.lower().replace('\\n', ' ')
        
        # Look for "balanstotaal", "belegd vermogen", "totale beleggingen"
        # usually in millions or thousands
        patterns = [
            r'(belegd vermogen|balanstotaal|totale beleggingen|totaal belegd vermogen|beleggingen)(.{0,50}?)(?:€|eur)?\s*(\\d{1,4}(?:[\\.,]\\d{3})*(?:[\\.,]\\d{1,2})?)(?=\\s|m|miljoen|duizend|$)'
        ]
        
        matches = []
        for p in patterns:
            for m in re.finditer(p, text):
                val_str = m.group(3).replace('.', '').replace(',', '.')
                try:
                    val = float(val_str)
                    # Filter out years and tiny numbers
                    if val > 10 and not (2010 <= val <= 2030):
                        matches.append(val)
                except ValueError:
                    pass
                    
        if matches:
            # We assume it's reported in millions or thousands. 
            # If it's > 100,000, it's likely thousands (need to divide by 1,000,000 to get bn)
            # If it's between 100 and 100,000, it's likely millions (need to divide by 1,000 to get bn)
            best_val = matches[0]
            if len(matches) > 1:
                # take the most common or max? Let's just print matches
                best_val = max(matches)
                
            return best_val
    except Exception as e:
        print(f"Error {e}")
    return None

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    for fid in FUNDS_TO_CHECK:
        c.execute("SELECT name FROM funds WHERE id = ?", (fid,))
        res = c.fetchone()
        if not res: continue
        name = res[0]
        
        # Find PDF
        pdf_file = None
        for f in os.listdir(REPORTS_DIR):
            if f.startswith(f"{fid}_") and f.endswith('.pdf'):
                pdf_file = os.path.join(REPORTS_DIR, f)
                break
                
        if pdf_file:
            aum = extract_aum_from_pdf(pdf_file)
            print(f"[{fid}] {name}: Found raw number ~{aum} (Need to convert to Bn)")
        else:
            print(f"[{fid}] {name}: NO PDF FOUND")
            
if __name__ == "__main__":
    main()
