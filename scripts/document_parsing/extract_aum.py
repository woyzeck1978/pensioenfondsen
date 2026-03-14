import os
import sqlite3
import fitz  # PyMuPDF
import re
import argparse
import pandas as pd

def extract_text_from_pdf(filepath):
    try:
        doc = fitz.open(filepath)
        text = ""
        # Read max 20 pages for AUM since it's usually in the intro/key figures
        for page in doc[:20]:
            text += page.get_text()
        return text
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

def find_aum(text):
    if not text:
        return None
        
    text = text.lower()
    
    # Common keywords for AUM in Dutch annual reports
    keywords = [r'belegd vermogen', r'balanstotaal', r'beschikbaar vermogen', r'totaal belegd', 'vermogen van']
    
    for keyword in keywords:
        for match in re.finditer(keyword, text):
            start = match.end()
            # Look ahead in a window of 400 characters
            window = text[start:start+400]
            
            # Match formats like:
            # € 12,5 miljard
            # EUR 8,1 mrd
            # 5.400 miljoen
            # 1.250.000 (if table in thousands x € 1.000) - harder to parse, focusing on explicit millions/billions
            
            # Pattern for billions (miljard/mrd)
            mrd_match = re.search(r'(?:€|eur)?\s*([\d\.,]+)\s*(?:miljard|mrd|bn|billion)', window)
            if mrd_match:
                val_str = mrd_match.group(1).replace('.', '').replace(',', '.')
                try:
                    return float(val_str) # Already in billions
                except ValueError:
                    pass
                    
            # Pattern for millions (miljoen/mln)
            mln_match = re.search(r'(?:€|eur)?\s*([\d\.,]+)\s*(?:miljoen|mln|m|million)', window)
            if mln_match:
                val_str = mln_match.group(1).replace('.', '').replace(',', '.')
                try:
                    return float(val_str) / 1000 # Convert to billions
                except ValueError:
                    pass
    return None

def main():
    parser = argparse.ArgumentParser(description="Extract AUM from downloaded annual reports")
    parser.add_argument("--limit", type=int, help="Limit the number of reports to process")
    args = parser.parse_args()

    reports_dir = '../../data/annual_reports'
    db_path = '../../data/processed/pension_funds.db'

    if not os.path.exists(reports_dir):
        print(f"Directory not found: {reports_dir}.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    files = [f for f in os.listdir(reports_dir) if f.endswith('.pdf')]
    print(f"Found {len(files)} reports to analyze.")

    if args.limit:
        files = files[:args.limit]

    results = []
    
    for i, filename in enumerate(files):
        print(f"\n[{i+1}/{len(files)}] Analyzing {filename}...")
        
        try:
            fund_id = int(filename.split('_')[0])
        except ValueError:
            print(f"Skipping {filename}: Could not extract fund ID.")
            continue

        filepath = os.path.join(reports_dir, filename)
        text = extract_text_from_pdf(filepath)
        
        aum_bn = find_aum(text)
        
        if aum_bn is not None:
            print(f"Found AUM: € {aum_bn} billion")
            results.append((aum_bn, fund_id))
            
            # Update database
            cursor.execute('''
            UPDATE funds SET aum_euro_bn = ? WHERE id = ?
            ''', (aum_bn, fund_id))
            conn.commit()
        else:
            print("Could not find AUM in text.")

    conn.close()
    print(f"\nDone! Extracted AUM for {len(results)}/{len(files)} reports.")

if __name__ == "__main__":
    main()
