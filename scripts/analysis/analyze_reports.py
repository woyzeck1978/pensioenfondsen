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
        for page in doc:
            text += page.get_text()
        return text
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

def find_equity_allocation(text):
    # Search for common Dutch terms for equity/shares and percentage
    # Look for "aandelen" followed by numbers and "%" within a short window
    # Returns the first plausible percentage found
    
    if not text:
        return None
        
    text = text.lower()
    
    # Common keywords
    keywords = ['aandelen', 'zakelijke waarden', 'equity']
    
    for keyword in keywords:
        # Find all occurrences of the keyword
        for match in re.finditer(keyword, text):
            start = match.end()
            # Look ahead in a window of 500 characters
            window = text[start:start+500]
            
            # Simple regex to find percentages (e.g., 45%, 45,5%, 45.5%)
            pct_matches = re.finditer(r'(\d{1,2}(?:[.,]\d{1,2})?)\s*%', window)
            for pct_match in pct_matches:
                val_str = pct_match.group(1).replace(',', '.')
                try:
                    val = float(val_str)
                    if 0 < val <= 100:
                        return val
                except ValueError:
                    continue
    return None

def main():
    parser = argparse.ArgumentParser(description="Analyze downloaded annual reports for equity allocation")
    parser.add_argument("--limit", type=int, help="Limit the number of reports to process")
    args = parser.parse_args()

    reports_dir = '../data/reports'
    db_path = '../../data/processed/pension_funds.db'

    if not os.path.exists(reports_dir):
        print(f"Directory not found: {reports_dir}. Run the download script first.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table if not exists
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS equity_allocations_extracted (
        fund_id INTEGER PRIMARY KEY,
        allocation_pct REAL,
        source_file TEXT,
        FOREIGN KEY (fund_id) REFERENCES funds(id)
    )
    ''')
    conn.commit()

    files = [f for f in os.listdir(reports_dir) if f.endswith('.pdf')]
    print(f"Found {len(files)} reports to analyze.")

    if args.limit:
        files = files[:args.limit]

    results = []
    
    for i, filename in enumerate(files):
        print(f"\n[{i+1}/{len(files)}] Analyzing {filename}...")
        
        # Extract fund_id from filename
        try:
            fund_id = int(filename.split('_')[0])
        except ValueError:
            print(f"Skipping {filename}: Could not extract fund ID.")
            continue

        filepath = os.path.join(reports_dir, filename)
        text = extract_text_from_pdf(filepath)
        
        allocation = find_equity_allocation(text)
        
        if allocation is not None:
            print(f"Found equity allocation: {allocation}%")
            results.append((fund_id, allocation, filename))
            
            # Update database
            cursor.execute('''
            INSERT OR REPLACE INTO equity_allocations_extracted (fund_id, allocation_pct, source_file)
            VALUES (?, ?, ?)
            ''', (fund_id, allocation, filename))
            conn.commit()
        else:
            print("Could not find equity allocation in text.")

    conn.close()
    print(f"\nDone! Extracted allocations for {len(results)}/{len(files)} reports.")
    
    if results:
        df = pd.DataFrame(results, columns=['fund_id', 'allocation_pct', 'source_file'])
        df.to_csv('../../data/processed/extracted_allocations.csv', index=False)
        print("Saved results to ../data/extracted_allocations.csv")

if __name__ == "__main__":
    main()
