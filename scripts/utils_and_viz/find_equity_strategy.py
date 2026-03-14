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
    # Check if we already have a strategy for this fund
    query = """
        SELECT f.id, f.name 
        FROM funds f
        LEFT JOIN equity_strategy s ON f.id = s.fund_id
        WHERE s.id IS NULL
    """
    if limit:
        query += f" LIMIT {limit}"
    cursor.execute(query)
    funds = cursor.fetchall()
    conn.close()
    return funds

def extract_strategy(text):
    if not text:
        return None, None, None, None, None
        
    text = text.lower()
    
    alloc_pct = None
    man_style = None
    factor = None
    esg = None
    geo = None
    
    # 1. Allocation %
    pct_matches = re.findall(r'(\d{1,2}(?:[.,]\d{1,2})?)\s*%\s*(?:in\s+)?aandelen', text)
    if pct_matches:
        try:
            alloc_pct = float(pct_matches[0].replace(',', '.'))
        except ValueError:
            pass
            
    # 2. Management Style
    if 'passief' in text or 'indexvolgend' in text or 'tracker' in text:
        man_style = 'Passive'
    elif 'actief' in text or 'stock picking' in text:
        man_style = 'Active'
    
    # Check for mixed
    if 'actief' in text and ('passief' in text or 'indexvolgend' in text):
        man_style = 'Mixed'
        
    # 3. Factor Investing
    if 'factorbeleggen' in text or 'smart beta' in text or 'factoren' in text:
        factor = 'Factor'
    elif 'market cap' in text or 'marktkapitalisatie' in text:
        factor = 'Non-Factor'
        
    # 4. ESG
    if 'esg' in text or 'duurzaam' in text or 'uitsluiting' in text or 'klimaat' in text or 'groen' in text:
        esg = 'Yes'
        
    # 5. Geographic
    geo_list = []
    if 'wereldwijd' in text or 'globaal' in text or 'global' in text:
        geo_list.append('Global')
    else:
        if 'europa' in text or 'europees' in text:
            geo_list.append('Europe')
        if 'noord-amerika' in text or 'noord amerika' in text or 'verenigde staten' in text or 'usa' in text:
            geo_list.append('North America')
        if 'opkomende markten' in text or 'emerging markets' in text or 'azië' in text:
            geo_list.append('Emerging Markets')
        if 'nederland' in text:
            geo_list.append('Netherlands')
            
    if geo_list:
        geo = ', '.join(geo_list)
        
    return alloc_pct, man_style, factor, esg, geo

def search_equity_strategy(fund_name):
    # Strip special chars
    clean_name = fund_name.replace('/', ' ').replace('(', '').replace(')', '').replace('&', '')
    clean_name = ' '.join(clean_name.split())
    
    queries = [
        f'pensioenfonds {clean_name} aandelen beleid actief passief ESG',
        f'pensioenfonds {clean_name} aandelenportefeuille factorbeleggen duurzaam'
    ]
    
    best_alloc = None
    best_man = None
    best_factor = None
    best_esg = None
    best_geo = None
    
    for q in queries:
        print(f"  Query: {q}")
        try:
            results = DDGS().text(q, max_results=5)
            if not results:
                continue
                
            for r in results:
                snippet = r.get('body', '') + ' ' + r.get('title', '')
                alloc, man, factor, esg, geo = extract_strategy(snippet)
                
                if alloc and not best_alloc:
                    best_alloc = alloc
                if man and not best_man:
                    best_man = man
                if factor and not best_factor:
                    best_factor = factor
                if esg and not best_esg:
                    best_esg = esg
                if geo and not best_geo:
                    best_geo = geo
                    
        except Exception as e:
            print(f"  Error with DDGS: {e}")
            break
            
        time.sleep(2)
        
    return best_alloc, best_man, best_factor, best_esg, best_geo

def main():
    parser = argparse.ArgumentParser(description="Find detailed Equity Strategy via DuckDuckGo")
    parser.add_argument("--limit", type=int, help="Limit number of funds to process")
    args = parser.parse_args()

    db_path = '../../data/processed/pension_funds.db'

    funds = get_all_funds(db_path, limit=args.limit)
    print(f"Found {len(funds)} funds needing equity strategy analysis.")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    count_found = 0

    for i, (fund_id, fund_name) in enumerate(funds):
        print(f"\n[{i+1}/{len(funds)}] Processing [{fund_id}] {fund_name}...")
        
        alloc, man, factor, esg, geo = search_equity_strategy(fund_name)
        
        # Even if nothing found, we insert a null row so we don't query it again endlessly
        if any([alloc, man, factor, esg, geo]):
            count_found += 1
            print(f"  => Found traits: Alloc: {alloc}%, Style: {man}, Factor: {factor}, ESG: {esg}, Geo: {geo}")
            
        query = """
            INSERT INTO equity_strategy (fund_id, allocation_pct, management_style, factor_investing, esg_focus, geographic_allocation)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        cursor.execute(query, (fund_id, alloc, man, factor, esg, geo))
        conn.commit()
            
        time.sleep(3)

    conn.close()
    print(f"\nDone! Found equity strategy data for {count_found} funds.")

if __name__ == "__main__":
    main()
