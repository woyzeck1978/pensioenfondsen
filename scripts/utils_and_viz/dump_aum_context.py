import sqlite3
import fitz
import glob
import os

db_path = '../../data/processed/pension_funds.db'
conn = sqlite3.Connection(db_path)
c = conn.cursor()

ids = [47, 48, 70, 117, 124, 129]

for fund_id in ids:
    c.execute("SELECT name FROM funds WHERE id=?", (fund_id,))
    name = c.fetchone()[0]
    files = glob.glob(f'../data/reports/{fund_id}_*.pdf')
    if not files: continue
    
    doc = fitz.open(files[0])
    print(f"\n==========================================")
    print(f"FUND: {name} (ID: {fund_id})")
    print(f"==========================================")
    
    found = False
    for i in range(min(120, len(doc))):
        text = doc[i].get_text()
        text_lower = text.lower()
        if "totaal belegd vermogen" in text_lower or "balanstotaal" in text_lower or "pensioenfondsvermogen" in text_lower:
            lines = text.split('\n')
            for idx, line in enumerate(lines):
                if "totaal belegd vermogen" in line.lower() or "balanstotaal" in line.lower() or "pensioenfondsvermogen" in line.lower():
                    start = max(0, idx - 3)
                    end = min(len(lines), idx + 8)
                    print(f"--- Page {i} Context ---")
                    print('\n'.join(lines[start:end]))
                    found = True
                    break
        if found:
            break
