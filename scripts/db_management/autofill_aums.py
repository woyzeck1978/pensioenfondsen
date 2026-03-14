import sqlite3
import requests
import urllib.parse
from bs4 import BeautifulSoup
import re
import time

db_path = '../../data/processed/pension_funds.db'

def fetch_duckduckgo_html(query):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching DDG for {query}: {e}")
        return ""

def extract_aum(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    results = soup.find_all('a', class_='result__snippet')
    
    text_corpus = " ".join([res.get_text() for res in results]).lower()
    
    # Matches "2,4 miljard", "150 miljoen", "€ 1.2 miljard", etc.
    pattern = r'(?:vermogen|beheer|balans|eur|€)\w{0,20}\s+(\d{1,4}(?:[.,]\d{1,3})?)\s+(miljard|miljoen|mld|mln)'
    matches = re.finditer(pattern, text_corpus)
    
    possible_aums = []
    for match in matches:
        val_str = match.group(1).replace(',', '.')
        scale = match.group(2)
        try:
            val = float(val_str)
            if 'miljoen' in scale or 'mln' in scale:
                val = val / 1000.0  # Convert to billions
            possible_aums.append(val)
        except ValueError:
            pass
            
    if possible_aums:
        # Return the most frequent or max? Let's just return the first one that looks like a valid pension AUM
        # Filter out tiny or massive outliers
        valid = [v for v in possible_aums if 0.01 <= v <= 600.0]
        if valid:
            return valid[0]
            
    return None

def main():
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    c.execute("SELECT id, name FROM funds WHERE aum_euro_bn IS NULL")
    missing_aums = c.fetchall()
    
    print(f"Found {len(missing_aums)} funds missing AUM. Starting autofill extraction...")
    success_count = 0
    
    for fund_id, name in missing_aums:
        # Simplify name for search
        search_name = name.split('(')[0].strip()
        query = f"Pensioenfonds {search_name} belegd vermogen 2024 OR 2023"
        print(f"[{fund_id}] Searching AUM for: {search_name}...")
        
        html = fetch_duckduckgo_html(query)
        aum = extract_aum(html)
        
        if aum:
            print(f"  -> Extracted AUM: {aum} Billion EUR")
            c.execute("UPDATE funds SET aum_euro_bn = ? WHERE id = ?", (round(aum, 3), fund_id))
            success_count += 1
        else:
            print("  -> Extraction failed.")
            
        time.sleep(1.5) # Be nice to DDG
        
    conn.commit()
    conn.close()
    
    print(f"\nAutofill Sequence Complete! Successfully mapped {success_count}/{len(missing_aums)} AUMs.")

if __name__ == "__main__":
    main()
