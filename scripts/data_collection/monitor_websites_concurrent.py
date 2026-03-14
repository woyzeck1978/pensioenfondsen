import sqlite3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import argparse
import concurrent.futures

def get_db_connection():
    conn = sqlite3.connect('../../data/processed/pension_funds.db')
    conn.row_factory = sqlite3.Row
    return conn

def extract_links_from_page(url, soup, base_domain):
    """Extract and categorize links from a soup object."""
    discovered = []
    
    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text(strip=True)
        if not text:
            text = a.get('title', '')
            if not text:
                img = a.find('img')
                if img: text = img.get('alt', '')
        
        full_url = urljoin(url, href)
        parsed = urlparse(full_url)
        
        if base_domain not in parsed.netloc:
            continue
            
        doc_type = None
        
        if full_url.lower().endswith(('.pdf', '.docx', '.xlsx', '.doc')):
            doc_type = 'document'
        elif ('/nieuws/' in full_url.lower() or '/actueel/' in full_url.lower()) and full_url != url:
            path_parts = [p for p in parsed.path.split('/') if p]
            if len(path_parts) > 1:
                doc_type = 'news'
                
        if doc_type:
            discovered.append({
                'url': full_url,
                'title': text,
                'doc_type': doc_type
            })
            
    return discovered

def scan_fund_worker(fund_tuple):
    """Worker function for concurrent execution."""
    fund_id, fund_name, website_url, test_mode = fund_tuple
    
    if not website_url or website_url == "None":
        return fund_id, fund_name, []
        
    print(f"[Thread] Starting {fund_name}")
    
    if not website_url.startswith('http'):
        website_url = 'https://' + website_url
        
    base_domain = urlparse(website_url).netloc
    
    paths_to_check = [
        '/',
        '/nieuws',
        '/actueel',
        '/documenten',
        '/downloads',
        '/over-ons/nieuws',
        '/over-het-fonds/documenten'
    ]
    
    all_discovered = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    visited = set()
    
    for path in paths_to_check:
        target_url = urljoin(website_url, path)
        if target_url in visited: continue
        visited.add(target_url)
        
        try:
            response = requests.get(target_url, headers=headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                links = extract_links_from_page(target_url, soup, base_domain)
                all_discovered.extend(links)
        except Exception:
            pass # Suppress thread errors to keep console clean
            
    # Deduplicate within this single run
    unique_discovered = {}
    for item in all_discovered:
        unique_discovered[item['url']] = item
        
    return fund_id, fund_name, list(unique_discovered.values())

def main():
    parser = argparse.ArgumentParser(description="Pension Fund Web Scraper / Monitor")
    parser.add_argument('--test', action='store_true', help="Run in test mode on just 3 funds")
    args = parser.parse_args()

    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = "SELECT id, name, website FROM funds WHERE website IS NOT NULL AND website != '' AND website != 'None'"
    if args.test:
        query += " LIMIT 3"
        
    cursor.execute(query)
    funds = cursor.fetchall()
    conn.close() # Close DB before threading
    
    print(f"Starting CONCURRENT scan of {len(funds)} pension fund websites...")
    
    # Prepare tuples for mapping
    fund_tuples = [(f['id'], f['name'], f['website'], args.test) for f in funds]
    
    all_results = []
    
    # Use ThreadPoolExecutor to run 20 sites at once
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        for result in executor.map(scan_fund_worker, fund_tuples):
            all_results.append(result)
            
    # Re-open DB strictly for sequential insertions to avoid locks
    conn = get_db_connection()
    cursor = conn.cursor()
    
    total_new_docs = 0
    total_new_news = 0
    
    print("\n[DB] Committing findings to database...")
    for fund_id, fund_name, discovered_items in all_results:
        new_findings_for_fund = 0
        
        for item in discovered_items:
            url = item['url']
            cursor.execute("SELECT id FROM scraped_documents WHERE url = ?", (url,))
            if not cursor.fetchone():
                try:
                    cursor.execute(
                        "INSERT INTO scraped_documents (fund_id, url, title, doc_type) VALUES (?, ?, ?, ?)",
                        (fund_id, url, item['title'], item['doc_type'])
                    )
                    new_findings_for_fund += 1
                    if item['doc_type'] == 'document': total_new_docs += 1
                    elif item['doc_type'] == 'news': total_new_news += 1
                except sqlite3.IntegrityError:
                    pass
        
        conn.commit()
        if new_findings_for_fund > 0:
            print(f"  -> {fund_name}: Inserted {new_findings_for_fund} new backlog items.")
            
    print("\n========================================================")
    print("BACKLOG INITIALIZATION COMPLETE.")
    print(f"Total New Documents Cataloged: {total_new_docs}")
    print(f"Total New News Articles Cataloged: {total_new_news}")
    print("========================================================")
    
    conn.close()

if __name__ == "__main__":
    main()
