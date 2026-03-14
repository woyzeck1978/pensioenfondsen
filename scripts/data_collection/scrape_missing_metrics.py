import sqlite3
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
import re
import urllib.parse

base_dir = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen"
db_path = os.path.join(base_dir, "data/processed/pension_funds.db")

def find_missing_funds():
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("""
        SELECT id, name, website 
        FROM funds 
        WHERE (aum_euro_bn IS NULL OR equity_allocation_pct IS NULL)
        AND website IS NOT NULL AND status NOT LIKE '%Opgeheven%' 
        AND status NOT LIKE '%Liquidatie%' AND status != 'Overdracht'
    """, conn)
    conn.close()
    return df

def extract_aum(text):
    # Looking for phrases like: belegd vermogen 5,3 miljard, of 120 miljoen
    match = re.search(r'(?:belegd vermogen|vermogen van|pensioenvermogen)\s*(?:van\s*)?(?:ruim\s*|ongeveer\s*|circa\s*)?€?\s*([\d,.]+)\s*(miljoen|miljard)', text, re.IGNORECASE)
    if match:
        val_str = match.group(1).replace(',', '.')
        try:
            val = float(val_str)
            if match.group(2).lower() == 'miljoen':
                return round(val / 1000.0, 3) 
            return round(val, 3) # miljard
        except ValueError:
            pass
    return None

def extract_equity(text):
    # Looking for 'Aandelen: 30%' or 'in aandelen 30,5%'
    match = re.search(r'aandelen[^\d]{0,20}([\d,.]+)\s*%', text, re.IGNORECASE)
    if match:
        val_str = match.group(1).replace(',', '.')
        try:
            return float(val_str)
        except ValueError:
            pass
    return None

def scrape_fund_metrics():
    df_missing = find_missing_funds()
    print(f"Targeting {len(df_missing)} active funds with missing AUM or Equity metrics.")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
    }
    
    updated_aum_count = 0
    updated_equity_count = 0
    
    # We will just scrape the homepage for now, as top-level metrics are often placed there
    for idx, row in df_missing.iterrows():
        f_id = row['id']
        name = row['name']
        url = row['website']
        
        # skip linkedins
        if 'linkedin' in str(url).lower(): continue
        
        try:
            if not str(url).startswith('http'): url = 'http://' + url
            response = requests.get(url, headers=headers, timeout=5)
            # if redirect to standard 403 or 404, break
            if response.status_code != 200: continue
            
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.get_text(separator=' ', strip=True)
            
            # Simple fallback: scrape 'beleggingen' or 'over-ons' page if available
            nav_links = soup.find_all('a', href=True)
            secondary_url = None
            for link in nav_links:
                href = link['href'].lower()
                if 'belegg' in href or 'vermogen' in href or 'kengetallen' in href or 'over-ons' in href or 'profiel' in href:
                    secondary_url = urllib.parse.urljoin(url, link['href'])
                    break
            
            if secondary_url:
                try:
                    res2 = requests.get(secondary_url, headers=headers, timeout=5)
                    if res2.status_code == 200:
                        soup2 = BeautifulSoup(res2.text, 'html.parser')
                        text += " " + soup2.get_text(separator=' ', strip=True)
                except Exception:
                    pass
            
            extracted_aum = extract_aum(text)
            extracted_equity = extract_equity(text)
            
            if extracted_aum:
                cursor.execute('UPDATE funds SET aum_euro_bn = ? WHERE id = ? AND aum_euro_bn IS NULL', (extracted_aum, f_id))
                updated_aum_count += cursor.rowcount
            if extracted_equity:
                cursor.execute('UPDATE funds SET equity_allocation_pct = ? WHERE id = ? AND equity_allocation_pct IS NULL', (extracted_equity, f_id))
                updated_equity_count += cursor.rowcount
                
        except Exception as e:
            # Silently pass on timeout / connection errors to not clutter logs
            pass

    conn.commit()
    conn.close()
    
    print(f"Scraping completed. Found and updated {updated_aum_count} AUM metrics and {updated_equity_count} Equity metrics.")

if __name__ == '__main__':
    scrape_fund_metrics()
