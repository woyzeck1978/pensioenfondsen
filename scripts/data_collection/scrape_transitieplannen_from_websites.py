import os
import sqlite3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
from fake_useragent import UserAgent

DB_PATH = "../../data/processed/pension_funds.db"
DOWNLOAD_DIR = "../../data/transitieplannen"

def download_file(url, filepath):
    ua = UserAgent()
    headers = {'User-Agent': ua.random}
    try:
        r = requests.get(url, headers=headers, stream=True, timeout=15)
        r.raise_for_status()
        with open(filepath, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        
        # Verify it's a PDF
        with open(filepath, 'rb') as f:
            header = f.read(5)
            if header != b'%PDF-':
                os.remove(filepath)
                return False
        return True
    except Exception as e:
        if os.path.exists(filepath):
            os.remove(filepath)
        return False

def scrape_website(fund_id, website_url):
    extensions = ['', '/documenten', '/downloads', '/over-ons', '/ons-fonds', '/pensioenakkoord', '/wtp', '/transitie', '/nieuwsPensioenakkoord']
    ua = UserAgent()
    
    for ext in extensions:
        url = website_url.rstrip('/') + ext
        try:
            print(f"Crawling {url}...")
            headers = {'User-Agent': ua.random}
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                continue
                
            soup = BeautifulSoup(response.content, 'html.parser')
            for a in soup.find_all('a', href=True):
                href = a['href']
                
                # Check for PDF extension and keyword
                if '.pdf' in href.lower() and ('transitie' in href.lower() or 'transitiedocument' in href.lower() or 'transitieplan' in href.lower()):
                    full_url = urljoin(url, href)
                    
                    # Prevent tracking links or self-referential downloads
                    if 'google' in full_url or 'mailto:' in full_url:
                        continue
                        
                    filename = f"{fund_id}_transitieplan.pdf"
                    filepath = os.path.join(DOWNLOAD_DIR, filename)
                    
                    print(f"[{fund_id}] Found potential file at: {full_url}")
                    if download_file(full_url, filepath):
                        print(f"[{fund_id}] Successfully downloaded Transitieplan!")
                        return True
                        
        except Exception as e:
            # print(f"Error accessing {url}: {e}")
            pass
            
        time.sleep(1) # Be polite
        
    return False

def main():
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT id, name, website FROM funds WHERE transitieplan_downloaded = 0 OR transitieplan_downloaded IS NULL")
    funds = c.fetchall()
    
    for fund_id, name, website in funds:
        if not website:
            print(f"[{fund_id}] Skipping {name} - No website listed.")
            continue
            
        print(f"\n--- Processing {name} (ID: {fund_id}) ---")
        success = scrape_website(fund_id, website)
        
        if success:
            c.execute("UPDATE funds SET transitieplan_downloaded = 1 WHERE id = ?", (fund_id,))
            conn.commit()
            
    conn.close()

if __name__ == '__main__':
    main()
