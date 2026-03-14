import sqlite3
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
import re
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

base_dir = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen"
db_path = os.path.join(base_dir, "data/processed/pension_funds.db")

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
}

def get_target_funds():
    conn = sqlite3.connect(db_path)
    # We want to scrape funds that are actively transitioning and don't have toeslag verlening mapped yet, or don't have a transitieplan URL
    df = pd.read_sql_query("""
        SELECT id, name, website 
        FROM funds 
        WHERE (toeslag_actieven_2025_pct IS NULL OR transitieplan_url IS NULL)
        AND website IS NOT NULL AND status NOT LIKE '%Opgeheven%' 
        AND status NOT LIKE '%Liquidatie%' AND status != 'Overdracht'
    """, conn)
    conn.close()
    return df

def extract_indexation(text):
    # Looking for: "verhogen met X,XX%" or "indexatie van X%"
    matches = re.finditer(r'(?:verhog(?:ing|en)|indexatie|toeslag|verhoogd).*?(?:met\s*)?([\d,.]+)\s*%', text, re.IGNORECASE)
    
    best_match = None
    for m in matches:
        val_str = m.group(1).replace(',', '.')
        try:
            val = float(val_str)
            # Filter out obvious false positives
            if 0.0 <= val <= 10.0:
                # We usually want the lowest valid reading near 'pensioen' to avoid extreme historical references
                best_match = val
                break # Just take the first valid hit for now
        except ValueError:
            pass
    return best_match

def process_fund(row):
    f_id = row['id']
    name = row['name']
    url = row['website']
    
    if 'linkedin' in str(url).lower(): return None
    
    if not str(url).startswith('http'): url = 'http://' + url
    
    updates = {}
    try:
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code != 200: return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 1. Look for Transitieplan URLs
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            if 'transitieplan' in href and href.endswith('.pdf'):
                full_url = urllib.parse.urljoin(url, link['href'])
                updates['transitieplan_url'] = full_url
                break
                
        # 2. Look for Indexation on Homepage
        text = soup.get_text(separator=' ', strip=True)
        idx = extract_indexation(text)
        if idx is not None:
            updates['toeslag'] = idx
            
        # 3. If no indexation found, check news/actueel page
        if 'toeslag' not in updates:
            news_url = None
            for link in soup.find_all('a', href=True):
                href = link['href'].lower()
                if 'nieuws' in href or 'actueel' in href:
                    news_url = urllib.parse.urljoin(url, link['href'])
                    break
            
            if news_url:
                try:
                    res2 = requests.get(news_url, headers=headers, timeout=5)
                    if res2.status_code == 200:
                        soup2 = BeautifulSoup(res2.text, 'html.parser')
                        text2 = soup2.get_text(separator=' ', strip=True)
                        idx2 = extract_indexation(text2)
                        if idx2 is not None:
                            updates['toeslag'] = idx2
                except Exception:
                    pass
                    
        if updates:
            updates['id'] = f_id
            return updates
            
    except Exception:
        pass
        
    return None

def run_scraper():
    df_missing = get_target_funds()
    print(f"Targeting {len(df_missing)} funds for Indexation 2025 and Transitieplan URLs...")
    
    results = []
    # Utilize threading to speed up the massive number of HTTP blocks
    with ThreadPoolExecutor(max_workers=10) as executor:
        for result in executor.map(process_fund, [row for _, row in df_missing.iterrows()]):
            if result:
                results.append(result)
                
    if not results:
        print("Scraping finished. No new updates found.")
        return
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    idx_count = 0
    tp_count = 0
    
    for res in results:
        f_id = res['id']
        
        if 'toeslag' in res:
            # We assume active and inactive get the same generically if not specified, mapped against 'toeslag_actieven_2025_pct'
            cursor.execute('''
                UPDATE funds 
                SET toeslag_actieven_2025_pct = ?, toeslag_gewezen_2025_pct = ? 
                WHERE id = ? AND toeslag_actieven_2025_pct IS NULL
            ''', (res['toeslag'], res['toeslag'], f_id))
            idx_count += cursor.rowcount
            
        if 'transitieplan_url' in res:
            cursor.execute('''
                UPDATE funds 
                SET transitieplan_url = ? 
                WHERE id = ? AND transitieplan_url IS NULL
            ''', (res['transitieplan_url'], f_id))
            tp_count += cursor.rowcount
            
    conn.commit()
    conn.close()
    
    print(f"Scraping completed. Found {idx_count} new 2025 Indexation values and {tp_count} Transitieplan URLs.")

if __name__ == '__main__':
    run_scraper()
