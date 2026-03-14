import os
import sqlite3
import requests
import urllib.parse
from bs4 import BeautifulSoup
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from fake_useragent import UserAgent

DB_PATH = "../../data/processed/pension_funds.db"
PLANS_DIR = "../../data/transitieplannen"

os.makedirs(PLANS_DIR, exist_ok=True)
ua = UserAgent()

def download_pdf(url, filepath):
    """Download a PDF, verifying it's actually a PDF."""
    try:
        # Use a random user agent to minimize 403s
        headers = {'User-Agent': ua.random}
        response = requests.get(url, stream=True, timeout=15, headers=headers)
        response.raise_for_status()
        
        # Verify content type or PDF magic number early
        content_type = response.headers.get('content-type', '').lower()
        if 'application/pdf' not in content_type:
            # Check the first few bytes just in case
            first_bytes = response.raw.read(5)
            if not first_bytes.startswith(b'%PDF-'):
                return False
            # Rewind isn't always possible on raw streams so we just accept it if magic bytes match

        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk: f.write(chunk)
        
        # Final sanity check on file size
        if os.path.getsize(filepath) < 10000: # Needs to be > 10kb to be a real transitieplan
            os.remove(filepath)
            return False
            
        return True
    except Exception as e:
        return False

from duckduckgo_search import DDGS

def search_duckduckgo(query):
    """Search using DDGS api which is much more robust against bot protection."""
    try:
        results = []
        # Fallback to duckduckgo_search API
        with DDGS() as ddgs:
            # Add a small sleep to prevent rapid API exhaustion
            time.sleep(1)
            for r in ddgs.text(query, max_results=8):
                if '.pdf' in r['href'].lower():
                    results.append(r['href'])
        return results
    except Exception as e:
        print(f"DDGS error: {e}")
        return []

def search_searx(query):
    """Query a public Searx instance. Rate limits easily, so fallback to DDG."""
    encoded_query = urllib.parse.quote(query)
    # Using a known reliable public instance, but may still fail
    search_url = f"https://searx.be/search?q={encoded_query}&format=json"
    
    try:
        resp = requests.get(search_url, timeout=10, headers={'User-Agent': ua.random})
        if resp.status_code == 200:
            data = resp.json()
            return [res['url'] for res in data.get('results', []) if res['url'].lower().endswith('.pdf')]
    except Exception:
        pass
    return []

def worker(fund_id, fund_name):
    """Isolated worker function to process a single fund."""
    filename = f"{fund_id}_{fund_name.replace(' ', '_').replace('/', '_')}.pdf"
    filepath = os.path.join(PLANS_DIR, filename)
    
    # Skip if we already downloaded it successfully
    if os.path.exists(filepath) and os.path.getsize(filepath) > 10000:
        return fund_id, True

    # Sleep jitter to avoid aggroing search engines
    time.sleep(random.uniform(0.5, 3.0))
    
    # Primary Search Query Structure
    query = f'"{fund_name}" transitieplan filetype:pdf'
    
    print(f"[{fund_id}] Searching: {query}")
    pdf_urls = search_duckduckgo(query)
    
    # If DuckDuckGo HTML blocked us or returned 0, try searx
    if not pdf_urls:
         pdf_urls = search_searx(query)

    # If both failed, try a looser query
    if not pdf_urls:
         loose_query = f'{fund_name} transitieplan filetype:pdf'
         pdf_urls = search_duckduckgo(loose_query)

    # Attempt downloads
    for url in pdf_urls[:3]: # Only try top 3 pdf links
        print(f"[{fund_id}] Trying download: {url[:60]}...")
        if download_pdf(url, filepath):
            print(f"[{fund_id}]  -> SUCCESS")
            return fund_id, True
            
    print(f"[{fund_id}]  -> FAILED to find a valid PDF transitieplan.")
    return fund_id, False

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT id, name FROM funds")
    funds = c.fetchall()

    success_count = 0
    total = len(funds)
    
    print(f"Processing {total} pension funds for Transition Plans...")

    # Using 4 threads to be polite to DuckDuckGo and the fund websites
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(worker, fund_id, name): fund_id for fund_id, name in funds}
        
        for future in as_completed(futures):
            fund_id = futures[future]
            try:
                result_id, success = future.result()
            except Exception as e:
                print(f"[{fund_id}]  -> FAILED with error: {e}")
                success = False

            # Update database record explicitly to True/False
            c.execute("UPDATE funds SET transitieplan_downloaded = ? WHERE id = ?", (1 if success else 0, fund_id))
            conn.commit()
            
            if success:
                success_count += 1
                
    print(f"\\nFinished processing. Downloaded {success_count} / {total} Transition Plans.")
    conn.close()

if __name__ == "__main__":
    main()
