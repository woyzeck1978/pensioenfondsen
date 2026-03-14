import sqlite3
import time
from ddgs import DDGS
import argparse
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_corporate_funds(db_path, limit=None):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = "SELECT id, name FROM funds WHERE category = 'Corporate'"
    if limit:
        query += f" LIMIT {limit}"
    cursor.execute(query)
    funds = cursor.fetchall()
    conn.close()
    return funds

def search_website(fund_name):
    query = f'"{fund_name}" pensioenfonds site:.nl'
    print(f"Searching for: {query}")
    try:
        results = DDGS().text(query, max_results=3)
        for r in results:
            href = r.get('href', '').lower()
            # Basic checks to avoid random pdfs or news
            if 'pdf' not in href and 'nieuws' not in href and 'dnb.nl' not in href:
                return r['href']
    except Exception as e:
        print(f"Error searching for {fund_name}: {e}")
    return None

def main():
    parser = argparse.ArgumentParser(description="Find websites for corporate pension funds")
    parser.add_argument("--limit", type=int, help="Limit the number of funds to process")
    args = parser.parse_args()

    db_path = '../../data/processed/pension_funds.db'

    funds = get_corporate_funds(db_path, limit=args.limit)
    print(f"Found {len(funds)} corporate funds to process.")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    results = []
    success_count = 0

    for i, (fund_id, fund_name) in enumerate(funds):
        print(f"\n[{i+1}/{len(funds)}] Processing [{fund_id}] {fund_name}...")
        
        url = search_website(fund_name)
        
        if url:
            print(f"Found URL: {url}")
            success_count += 1
            results.append((fund_id, fund_name, url))
            
            # Update database
            cursor.execute('''
            UPDATE funds SET website = ? WHERE id = ?
            ''', (url, fund_id))
            conn.commit()
        else:
            print("No suitable website found.")
        
        # Be nice to the search engine
        time.sleep(2)

    conn.close()
    print(f"\nDone! Found websites for {success_count}/{len(funds)} corporate funds.")

    # Export to CSV
    if results:
        df = pd.DataFrame(results, columns=['id', 'name', 'website'])
        df.to_csv('../data/corporate_funds_websites.csv', index=False)
        print("Saved results to ../data/corporate_funds_websites.csv")

if __name__ == "__main__":
    main()
