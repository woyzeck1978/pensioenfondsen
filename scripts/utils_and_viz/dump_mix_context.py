import sqlite3
import fitz
import glob
import os

db_path = '../../data/processed/pension_funds.db'
conn = sqlite3.Connection(db_path)
c = conn.cursor()

c.execute("SELECT id, name FROM funds WHERE category='Corporate' AND (beleggingsmix LIKE 'Unknown%' OR beleggingsmix LIKE 'Mentions mix%')")
funds = c.fetchall()

def find_pdf_for_fund(fund_id):
    files = glob.glob(f'../data/reports/{fund_id}_*.pdf')
    return files[0] if files else None

for fund_id, name in funds:
    pdf_path = find_pdf_for_fund(fund_id)
    if not pdf_path:
        print(f"[{name}] No PDF found.")
        continue
    
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        print(f"[{name}] Failed to open PDF.")
        continue
        
    print(f"\n==========================================")
    print(f"FUND: {name} (ID: {fund_id})")
    print(f"==========================================")
    
    found = False
    for i in range(min(120, len(doc))):
        text = doc[i].get_text()
        text_lower = text.lower()
        if "aandelen" in text_lower and ("vastrentend" in text_lower or "obligaties" in text_lower) and ("%" in text_lower):
            lines = text.split('\n')
            for idx, line in enumerate(lines):
                if "aandelen" in line.lower() or "vastrentend" in line.lower() or "obligaties" in line.lower():
                    start = max(0, idx - 5)
                    end = min(len(lines), idx + 10)
                    print(f"--- Page {i} Context ---")
                    print('\n'.join(lines[start:end]))
                    found = True
                    break
        if found:
            break
            
    if not found:
        print("No explicit strict asset allocation table found in first 120 pages.")
