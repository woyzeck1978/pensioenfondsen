import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import urllib.parse
import os

base_dir = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen"
db_path = os.path.join(base_dir, "data/processed/pension_funds.db")

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
}

def get_target_funds():
    conn = sqlite3.connect(db_path)
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
    matches = re.finditer(r'(?:verhog(?:ing|en)|indexatie|toeslag|verhoogd).*?(?:met\s*)?([\d,.]+)\s*%', text, re.IGNORECASE)
    best_match = None
    for m in matches:
        val_str = m.group(1).replace(',', '.')
        try:
            val = float(val_str)
            if 0.0 <= val <= 10.0:
                best_match = val
                break
        except ValueError:
            pass
    return best_match

def process_fund(row):
    f_id = row['id']
    name = row['name']
    url = row['website']
    
    if 'linkedin' in str(url).lower(): return None
    if not str(url).startswith('http'): url = 'http://' + url
    
    print(f"[{name}] Checking {url}...")
    updates = {}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200: 
            print(f"  -> Error: HTTP {response.status_code}")
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            if 'transitieplan' in href and href.endswith('.pdf'):
                full_url = urllib.parse.urljoin(url, link['href'])
                updates['transitieplan_url'] = full_url
                print(f"  -> Found Transitieplan: {full_url}")
                break
                
        text = soup.get_text(separator=' ', strip=True)
        idx = extract_indexation(text)
        if idx is not None:
            updates['toeslag'] = idx
            print(f"  -> Found Indexation (hp): {idx}%")
            
        if 'toeslag' not in updates:
            news_url = None
            for link in soup.find_all('a', href=True):
                href = link['href'].lower()
                if 'nieuws' in href or 'actueel' in href:
                    news_url = urllib.parse.urljoin(url, link['href'])
                    break
            
            if news_url:
                print(f"  -> Checking News page: {news_url}")
                try:
                    res2 = requests.get(news_url, headers=headers, timeout=10)
                    if res2.status_code == 200:
                        soup2 = BeautifulSoup(res2.text, 'html.parser')
                        text2 = soup2.get_text(separator=' ', strip=True)
                        idx2 = extract_indexation(text2)
                        if idx2 is not None:
                            updates['toeslag'] = idx2
                            print(f"  -> Found Indexation (news): {idx2}%")
                except requests.exceptions.Timeout:
                    print("  -> News page timeout.")
                except Exception as e:
                    print(f"  -> News page error {e}")
                    
        if updates:
            updates['id'] = f_id
            return updates
            
    except requests.exceptions.Timeout:
         print(f"  -> Timeout reached for main url.")
    except Exception as e:
        print(f"  -> Error: {e}")
        
    return None

def run_scraper():
    df_missing = get_target_funds()
    print(f"Targeting {len(df_missing)} funds for Indexation 2025 and Transitieplan URLs synchronously...")
    
    results = []
    for _, row in df_missing.iterrows():
        result = process_fund(row)
        if result:
            results.append(result)
                
    if not results:
        print("Scraping finished. No new updates found.")
        return
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    idx_count = tp_count = 0
    
    for res in results:
        f_id = res['id']
        if 'toeslag' in res:
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
