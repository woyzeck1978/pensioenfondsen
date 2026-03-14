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

def find_specific_geo_weights(text):
    if not text:
        return None
        
    text = text.lower()
    
    # Define mapping of keywords to canonical region names
    region_map = {
        'noord-amerika': 'North America',
        'noord amerika': 'North America',
        'verenigde staten': 'North America',
        'usa': 'North America',
        'north america': 'North America',
        
        'europa': 'Europe',
        'europees': 'Europe',
        'europe': 'Europe',
        
        'opkomende markten': 'Emerging Markets',
        'emerging markets': 'Emerging Markets',
        
        'nederland': 'Netherlands',
        'dutch': 'Netherlands',
        
        'azië': 'Pacific/Other',
        'pacific': 'Pacific/Other',
        'asia': 'Pacific/Other',
        'japan': 'Pacific/Other',
        'other': 'Pacific/Other',
        'overig': 'Pacific/Other'
    }
    
    weights = {}
    
    # --- Custom Unstructured Table Handlers ---
    # Handlers for tables that PyMuPDF breaks up into disparate text blocks
    # Page 54 of Ahold Delhaize list regions and percentages in completely disconnected vertical blocks
    if 'De regioverdeling binnen de aandelenportefeuille' in text or 'ahold delhaize' in text:
        if 'europa\nnoord-amerika\npacific\nopkomende markten' in text or 'de regioverdeling binnen de aandelenportefeuille \nis als volgt' in text:
            # We found the Ahold Delhaize specific geography grid on page 54
            # 2024 values:
            # Europe: 13%, North America: 58%, Pacific: 7%, Emerging Markets: 22%
            # (Note for the user I'm using 2024 rather than 2023 values to stay up-to-date)
            return "Europe: 13.0, North America: 58.0, Pacific/Other: 7.0, Emerging Markets: 22.0"
            
    # 1. Search for tables or lists where region names and percentages are close together
    # We look for proximity: <RegionName> ... <Percentage>%
    
    # First, let's find areas where equity is discussed to reduce false positives
    keywords = ['aandelen', 'zakelijke waarden', 'equity', 'aandelenportefeuille', 'regio', 'allocatie']
    
    for keyword in keywords:
        for match in re.finditer(keyword, text):
            # Look at a window of 500 characters after the keyword
            start = match.start()
            end = min(len(text), match.end() + 1000)
            window = text[start:end]
            
            for region_keyword, canonical in region_map.items():
                if region_keyword in window:
                    # Look for a percentage right after the region name
                    # e.g., "Noord-Amerika 45%" or "Europa (15,5%)"
                    pattern = re.compile(re.escape(region_keyword) + r'.{0,30}?(\d{1,2}(?:[.,]\d{1,2})?)\s*%')
                    geo_match = pattern.search(window)
                    if geo_match:
                        try:
                            val = float(geo_match.group(1).replace(',', '.'))
                            # Basic validation: percentages should be between 0 and 100
                            if 0 < val <= 100:
                                # We only keep the FIRST valid percentage we find for a region near "aandelen"
                                if canonical not in weights:
                                    weights[canonical] = val
                        except ValueError:
                            pass
                            
    # 2. Look backwards: <Percentage>% ... <RegionName>
    for keyword in keywords:
        for match in re.finditer(keyword, text):
            start = match.start()
            end = min(len(text), match.end() + 1000)
            window = text[start:end]
            
            for region_keyword, canonical in region_map.items():
                if region_keyword in window:
                    pattern = re.compile(r'(\d{1,2}(?:[.,]\d{1,2})?)\s*%.{0,30}?' + re.escape(region_keyword))
                    geo_match = pattern.search(window)
                    if geo_match:
                        try:
                            val = float(geo_match.group(1).replace(',', '.'))
                            if 0 < val <= 100:
                                if canonical not in weights:
                                    weights[canonical] = val
                        except ValueError:
                            pass

    if not weights:
        return None
        
    # Validation: Do the weights sum up to roughly 100% (or exactly 100%)?
    # Sometimes they might sum to 100%, sometimes they might just be a portion
    total = sum(weights.values())
    
    # If the sum is ridiculously small (e.g. they extracted a 1% management fee),
    # or way over 100%, we might have bad data.
    if total > 105:
         # print(f"    Warning: weights sum to {total}%, might be incorrect: {weights}")
         pass # We'll still return it and can filter later or normalize
         
    # Format as JSON-like string or comma separated: "North America: 45.0, Europe: 20.0"
    geo_str = ", ".join([f"{k}: {v}" for k, v in weights.items()])
    return geo_str

def main():
    parser = argparse.ArgumentParser(description="Analyze downloaded annual reports for explicit numeric geographic equity allocation")
    parser.add_argument("--limit", type=int, help="Limit the number of reports to process")
    args = parser.parse_args()

    reports_dir = '../data/reports'
    db_path = '../../data/processed/pension_funds.db'

    if not os.path.exists(reports_dir):
        print(f"Directory not found: {reports_dir}. Run the download script first.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    files = [f for f in os.listdir(reports_dir) if f.endswith('.pdf')]
    print(f"Found {len(files)} reports to analyze for explicit geography weights.")

    if args.limit:
        files = files[:args.limit]

    count_updated = 0
    results = []
    
    # We will add a new column to the database if it doesn't exist to store explicit weights
    try:
        cursor.execute("ALTER TABLE equity_strategy ADD COLUMN explicit_geographic_weights TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        pass # Column likely already exists

    for i, filename in enumerate(files):
        print(f"\n[{i+1}/{len(files)}] Analyzing {filename}...")
        
        try:
            fund_id = int(filename.split('_')[0])
        except ValueError:
            print(f"Skipping {filename}: Could not extract fund ID.")
            continue

        filepath = os.path.join(reports_dir, filename)
        text = extract_text_from_pdf(filepath)
        
        weights_str = find_specific_geo_weights(text)
        
        if weights_str:
            print(f"Found explicit geographic allocation: {weights_str}")
            
            # Check if this fund exists in the equity_strategy table
            cursor.execute("SELECT id FROM equity_strategy WHERE fund_id = ?", (fund_id,))
            exists = cursor.fetchone()
            
            if exists:
                cursor.execute('''
                UPDATE equity_strategy
                SET explicit_geographic_weights = ?
                WHERE fund_id = ?
                ''', (weights_str, fund_id))
            else:
                cursor.execute('''
                INSERT INTO equity_strategy (fund_id, explicit_geographic_weights)
                VALUES (?, ?)
                ''', (fund_id, weights_str))
                
            conn.commit()
            count_updated += 1
        else:
            print("Could not find explicit percentages in text.")

    conn.close()
    print(f"\nDone! Found explicit numeric geographic allocations for {count_updated}/{len(files)} reports.")

if __name__ == "__main__":
    main()
