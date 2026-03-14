import os
import sqlite3
import fitz  # PyMuPDF
import re
import argparse
import pandas as pd
from collections import Counter

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

def find_geographic_allocation(text):
    if not text:
        return None
        
    text = text.lower()
    
    # We want to find sections discussing equity allocation ("aandelen", "zakelijke waarden")
    # and then look for geographic terms nearby.
    keywords = ['aandelen', 'zakelijke waarden', 'equity', 'aandelenportefeuille', 'regio']
    
    geo_mentions = []
    
    for keyword in keywords:
        for match in re.finditer(keyword, text):
            # Look at a window of 300 characters before and after the keyword
            start = max(0, match.start() - 300)
            end = min(len(text), match.end() + 300)
            window = text[start:end]
            
            # Check for specific geographic labels in this window
            if 'wereldwijd' in window or 'globaal' in window or 'global' in window:
                geo_mentions.append('Global')
            if 'europa' in window or 'europees' in window or 'europe' in window:
                geo_mentions.append('Europe')
            if 'noord-amerika' in window or 'noord amerika' in window or 'verenigde staten' in window or 'usa' in window or 'north america' in window:
                geo_mentions.append('North America')
            if 'opkomende markten' in window or 'emerging markets' in window or 'azië' in window or 'pacific' in window or 'asia' in window or 'japan' in window:
                geo_mentions.append('Emerging Markets')
            if 'nederland' in window or 'dutch' in window:
                geo_mentions.append('Netherlands')
                
    if not geo_mentions:
        return None
        
    # Count the frequencies to find the dominant strategy
    counts = Counter(geo_mentions)
    
    # If "Global" is the most common or tied, it's likely a global fund.
    # Otherwise, returning the top 2-3 mentioned regions gives a good summary.
    top_geos = [item for item, count in counts.most_common() if count > 0]
    
    # If they mention NO/EU/EM frequently together, we can classify it as diversified
    if 'North America' in top_geos and 'Europe' in top_geos:
        if 'Global' not in top_geos:
            top_geos.insert(0, 'Global')
            
    # Remove Netherlands if Global is present to avoid clutter, unless it's the only one
    if 'Global' in top_geos and 'Netherlands' in top_geos and len(top_geos) > 2:
        top_geos.remove('Netherlands')
        
    return ', '.join(top_geos[:3]) # Return top 3 regions

def main():
    parser = argparse.ArgumentParser(description="Analyze downloaded annual reports for geographic equity allocation")
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
    print(f"Found {len(files)} reports to analyze for geography.")

    if args.limit:
        files = files[:args.limit]

    count_updated = 0
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
        
        geo = find_geographic_allocation(text)
        
        if geo:
            print(f"Found geographic allocation: {geo}")
            results.append({'fund_id': fund_id, 'geographic_allocation': geo})
            
            # Update database
            cursor.execute("SELECT id FROM equity_strategy WHERE fund_id = ?", (fund_id,))
            exists = cursor.fetchone()
            
            if exists:
                cursor.execute('''
                UPDATE equity_strategy
                SET geographic_allocation = ?
                WHERE fund_id = ?
                ''', (geo, fund_id))
            else:
                cursor.execute('''
                INSERT INTO equity_strategy (fund_id, geographic_allocation)
                VALUES (?, ?)
                ''', (fund_id, geo))
                
            conn.commit()
            count_updated += 1
        else:
            print("Could not find specific geographic details in text.")

    conn.close()
    print(f"\nDone! Updated geographic allocations for {count_updated}/{len(files)} reports.")

if __name__ == "__main__":
    main()
