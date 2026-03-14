import sqlite3
import os
import re
from pypdf import PdfReader

conn = sqlite3.connect('../../data/processed/pension_funds.db')
cursor = conn.cursor()

query = "SELECT id, name FROM funds WHERE data_source = 'DNB Appendix' AND annual_report_downloaded = 1 AND aum_euro_bn IS NULL"
cursor.execute(query)
funds = cursor.fetchall()

def extract_text(pdf_path):
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for i, page in enumerate(reader.pages):
            if i > 50: break
            t = page.extract_text()
            if t: text += t + "\n"
        return text
    except Exception as e:
        return ""

updates = 0
for fund_id, name in funds:
    safe_name = "".join(x for x in name if x.isalnum() or x in " -_").strip()
    pdf_path = f"../data/reports/{fund_id}_{safe_name}.pdf"
    
    if not os.path.exists(pdf_path):
        continue
        
    print(f"\nAnalyzing [{fund_id}] {name}...")
    text = extract_text(pdf_path).replace('\n', ' ')
    if not text:
        continue
        
    aum = None
    # 1. Match full numbers (e.g., 1.871.783.000)
    # 2. Match with mln, miljoen, mld, miljard
    patterns = [
        # Full Numbers > 10 million (e.g. 1.800.000.000)
        r"belegd vermogen[^\d]{1,30}(\d{1,3}(?:\.\d{3}){2,3})",
        # Abbreviations
        r"belegd vermogen[^\d]{1,30}(\d{1,4}(?:,\d{1,2})?)\s*(?:mln|miljoen|milj)",
        r"belegd vermogen[^\d]{1,30}(\d{1,3}(?:,\d{1,2})?)\s*(?:mld|miljard)",
        
        r"totaal belegd vermogen[^\d]{1,30}(\d{1,3}(?:\.\d{3}){2,3})",
        r"totaal belegd vermogen[^\d]{1,30}(\d{1,4}(?:,\d{1,2})?)\s*(?:mln|miljoen|milj)",
        r"totaal belegd vermogen[^\d]{1,30}(\d{1,3}(?:,\d{1,2})?)\s*(?:mld|miljard)",
        
        r"totale vermogen[^\d]{1,30}(\d{1,4}(?:,\d{1,2})?)\s*(?:mln|miljoen|milj)",
        r"totale vermogen[^\d]{1,30}(\d{1,3}(?:,\d{1,2})?)\s*(?:mld|miljard)",
    ]
    
    for p in patterns:
        match = re.search(p, text.lower())
        if match:
            raw_val = match.group(1).replace('.', '').replace(',', '.')
            val = float(raw_val)
            print(f"  Matched via RegExp: {match.group(0)}")
            
            # Determine scale
            context = match.group(0).lower()
            if 'mld' in context or 'miljard' in context:
                aum = val
            elif 'mln' in context or 'miljoen' in context or 'milj' in context:
                aum = val / 1000.0
            else:
                # It's a full number. E.g. 1871783000 -> 1.87 BN
                aum = val / 1000000000.0
                
            break

    print(f"  Extracted -> AUM: {aum} BN")
    
    if aum:
        cursor.execute("UPDATE funds SET aum_euro_bn=? WHERE id=?", (aum, fund_id))
        updates += 1

conn.commit()
conn.close()
print(f"\nSuccessfully populated granular AUM metrics from PDFs for {updates} appended funds.")
