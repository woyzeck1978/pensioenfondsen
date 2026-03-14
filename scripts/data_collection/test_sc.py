print("BOOTING SCRAPER SCRIPT")
import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
import urllib.parse
import os
import re
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

base_dir = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen"
db_path = os.path.join(base_dir, "data/processed/pension_funds.db")

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7',
}

ROLES = [
    'voorzitter', 'bestuurslid', 'directeur', 'ceo', 'cfo', 'cro',
    'secretaris', 'penningmeester', 'uitvoerend bestuurder', 'hoofd',
    'manager', 'lid bestuur', 'lid verantwoordingsorgaan', 'lid raad van toezicht',
    'sleutelfunctiehouder', 'directie'
]

PATHS_TO_CHECK = [
    "",
    "/over-ons",
    "/bestuur",
    "/organisatie",
    "/wie-zijn-wij",
    "/over-ons/bestuur",
    "/over-ons/organisatie",
    "/over-het-pensioenfonds/bestuur",
    "/organisatie/bestuur"
]

def get_target_funds():
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("""
        SELECT id, name, website 
        FROM funds 
        WHERE website IS NOT NULL 
        AND status NOT LIKE '%Opgeheven%' 
        AND status NOT LIKE '%Liquidatie%' 
        AND status NOT LIKE '%Closed/Liquidated%'
        AND status != 'Overdracht'
    """, conn)
    conn.close()
    return df

def extract_people_from_soup(soup, source_url):
    """Extracts people from structural tags like lists, tables, and short paragraphs."""
    people = []
    seen_names = set()
    
    tags_to_check = soup.find_all(['li', 'td', 'div', 'p'])
    
    for tag in tags_to_check:
        text = tag.get_text(separator=' | ', strip=True)
        text_lower = text.lower()
        
        # Fast exit if text is too long or empty
        if len(text) < 5 or len(text) > 150:
            continue
            
        matched_role = next((role for role in ROLES if role in text_lower), None)
        
        if matched_role:
            # We found a tag with a role. Let's try to extract the name.
            # Names are usually 2-4 words, capitalized.
            
            # Common formats: "J. Smit (Voorzitter)" or "Voorzitter: John Doe" or "Jane Doe, Bestuurslid"
            parts = re.split(r'[:|,\-(]', text)
            potential_names = []
            
            for part in parts:
                clean_part = part.replace(')', '').strip()
                # Check if this part looks like a name (not containing the role itself if it's multiple words)
                if len(clean_part.split()) >= 2 and len(clean_part.split()) <= 4 and matched_role not in clean_part.lower():
                    # Check for Title Case or initials
                    if bool(re.match(r'^([A-Z]\.?\s?)+[A-Z][a-z]+', clean_part)) or clean_part.istitle():
                        potential_names.append(clean_part)
            
            if potential_names:
                name = potential_names[0]
                n_key = name.lower()
                if n_key not in seen_names and len(n_key) > 5 and len(n_key) < 40:
                    people.append({'name': name, 'role': text[:100], 'source_url': source_url})
                    seen_names.add(n_key)
                    
    return people

def process_fund(row):
    f_id = row['id']
    name = row['name']
    base_url = row['website']
    
    if 'linkedin' in str(base_url).lower(): return []
    if not str(base_url).startswith('http'): base_url = 'http://' + base_url
    
    # Clean trailing slash
    base_url = base_url.rstrip('/')
    
    print(f"[{name}] Scanning for board members...")
    people_found = []
    
    # We will stop after finding the first page that yields people
    for path in PATHS_TO_CHECK:
        t_url = base_url + path
        try:
            res = requests.get(t_url, headers=headers, timeout=8, verify=False)
            if res.status_code == 200:
                soup = BeautifulSoup(res.text, 'html.parser')
                main_content = soup.find('main') or soup.find(id='content') or soup.body
                if main_content:
                    extracted = extract_people_from_soup(main_content, t_url)
                    if extracted:
                        print(f"  -> Found {len(extracted)} people on {t_url}")
                        for p in extracted:
                            p['fund_id'] = f_id
                            people_found.append(p)
                        break # Stop checking other paths if we found the board page
        except Exception:
            pass # Ignore timeouts or 404s
            
    return people_found

def run_scraper():
    df_funds = get_target_funds()
    print(f"Targeting {len(df_funds)} funds to scrape board members...")
    
    all_people = []
    
    for _, row in df_funds.iterrows():
        people = process_fund(row)
        if people:
            all_people.extend(people)
                
    if not all_people:
        print("Scraping finished. No people found.")
        return
        
    print(f"Found {len(all_people)} total individuals. Saving to database...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM fund_people")
    
    for p in all_people:
        cursor.execute('''
            INSERT INTO fund_people (fund_id, name, role, source_url)
            VALUES (?, ?, ?, ?)
        ''', (p['fund_id'], p['name'], p['role'], p['source_url']))
        
    conn.commit()
    conn.close()
    
    print(f"Successfully saved {len(all_people)} board members to the database.")

if __name__ == '__main__':
    run_scraper()
