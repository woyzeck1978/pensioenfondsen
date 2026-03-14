import sqlite3
import time
from ddgs import DDGS
import argparse
import pandas as pd
import re
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_all_funds(db_path, limit=None):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = "SELECT id, name FROM funds"
    if limit:
        query += f" LIMIT {limit}"
    cursor.execute(query)
    funds = cursor.fetchall()
    conn.close()
    return funds

def extract_metrics(text):
    if not text:
        return None, None
        
    text = text.lower()
    
    aum_bn = None
    dekkingsgraad = None
    
    # Extract AUM (billions)
    # Looking for: 12,5 miljard, 8,1 mrd, 5.400 miljoen
    mrd_match = re.search(r'(?:€|eur)?\s*([\d\.,]+)\s*(?:miljard|mrd|bn|billion)', text)
    if mrd_match:
        val_str = mrd_match.group(1).replace('.', '').replace(',', '.')
        try:
            aum_bn = float(val_str)
        except ValueError:
            pass
            
    if aum_bn is None:
        mln_match = re.search(r'(?:€|eur)?\s*([\d\.,]+)\s*(?:miljoen|mln|m|million)', text)
        if mln_match:
            val_str = mln_match.group(1).replace('.', '').replace(',', '.')
            try:
                aum_bn = float(val_str) / 1000
            except ValueError:
                pass
                
    # Extract Dekkingsgraad (percentage)
    # Looking for: 110,5%, 98.4 %
    # Using lookbehind for "dekkingsgraad" context or lookahead
    # Since search snippets are short, any percentage is a candidate if "dekkingsgraad" is in the text
    if 'dekkingsgraad' in text or 'beleidsdekkingsgraad' in text:
        pct_matches = re.findall(r'(\d{2,3}(?:[.,]\d{1,2})?)\s*%', text)
        if pct_matches:
            try:
                # take the first percentage found
                val_str = pct_matches[0].replace(',', '.')
                val = float(val_str)
                if 50 <= val <= 200:  # sanity check for coverage ratios
                    dekkingsgraad = val
            except ValueError:
                pass

    return aum_bn, dekkingsgraad

def search_fund_metrics(fund_name):
    # Strip special chars
    clean_name = fund_name.replace('/', ' ').replace('(', '').replace(')', '').replace('&', '')
    clean_name = ' '.join(clean_name.split())
    
    queries = [
        f'pensioenfonds {clean_name} actuele dekkingsgraad',
        f'pensioenfonds {clean_name} belegd vermogen miljard'
    ]
    
    best_aum = None
    best_dekkingsgraad = None
    
    for q in queries:
        print(f"  Query: {q}")
        try:
            results = DDGS().text(q, max_results=5)
            if not results:
                continue
                
            for r in results:
                snippet = r.get('body', '') + ' ' + r.get('title', '')
                aum, dekkingsgraad = extract_metrics(snippet)
                
                if aum and not best_aum:
                    best_aum = aum
                if dekkingsgraad and not best_dekkingsgraad:
                    best_dekkingsgraad = dekkingsgraad
                    
                if best_aum and best_dekkingsgraad:
                    break # Got both
        except Exception as e:
            print(f"  Error with DDGS: {e}")
            break
            
        if best_aum and best_dekkingsgraad:
            break
            
        time.sleep(2)
        
    return best_aum, best_dekkingsgraad

def main():
    parser = argparse.ArgumentParser(description="Find AUM and Dekkingsgraad via DuckDuckGo")
    parser.add_argument("--limit", type=int, help="Limit number of funds to process")
    args = parser.parse_args()

    db_path = '../../data/processed/pension_funds.db'

    funds = get_all_funds(db_path, limit=args.limit)
    print(f"Found {len(funds)} funds to process.")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    count_aum = 0
    count_dek = 0

    for i, (fund_id, fund_name) in enumerate(funds):
        print(f"\n[{i+1}/{len(funds)}] Processing [{fund_id}] {fund_name}...")
        
        aum, dek = search_fund_metrics(fund_name)
        
        updates = []
        params = []
        
        if aum is not None:
            print(f"  => Found AUM: € {aum:.3f} bn")
            updates.append("aum_euro_bn = ?")
            params.append(aum)
            count_aum += 1
            
        if dek is not None:
            print(f"  => Found Dekkingsgraad: {dek}%")
            updates.append("dekkingsgraad_pct = ?")
            params.append(dek)
            count_dek += 1
            
        if updates:
            params.append(fund_id)
            query = f"UPDATE funds SET {', '.join(updates)} WHERE id = ?"
            cursor.execute(query, tuple(params))
            conn.commit()
        else:
            print("  => No metrics found.")
            
        time.sleep(3)

    conn.close()
    print(f"\nDone! Found AUM for {count_aum} funds and Dekkingsgraad for {count_dek} funds.")

if __name__ == "__main__":
    main()
