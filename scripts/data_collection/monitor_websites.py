import sqlite3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import argparse

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
            # Try to get text from title attribute or alt text of child images
            text = a.get('title', '')
            if not text:
                img = a.find('img')
                if img: text = img.get('alt', '')
        
        full_url = urljoin(url, href)
        parsed = urlparse(full_url)
        
        # Only process URLs from the same domain (or very close subdomains) to avoid external noise
        if base_domain not in parsed.netloc:
            continue
            
        doc_type = None
        
        # 1. Detect PDFs and Documents
        if full_url.lower().endswith(('.pdf', '.docx', '.xlsx', '.doc')):
            doc_type = 'document'
        # 2. Detect News/Actueel articles (usually they don't have extensions)
        elif ('/nieuws/' in full_url.lower() or '/actueel/' in full_url.lower()) and full_url != url:
            # Basic check to ensure it's an article and not just the /nieuws/ index page
            path_parts = [p for p in parsed.path.split('/') if p]
            if len(path_parts) > 1: # e.g. /nieuws/some-article-title/
                doc_type = 'news'
                
        if doc_type:
            discovered.append({
                'url': full_url,
                'title': text,
                'doc_type': doc_type
            })
            
    return discovered

def scan_fund(fund_id, fund_name, website_url, conn):
    """Scrapes a specific fund's website for news and documents."""
    if not website_url or website_url == "None":
        return []
        
    print(f"\n[Scanning] {fund_name} ({website_url})")
    
    # Ensure scheme
    if not website_url.startswith('http'):
        website_url = 'https://' + website_url
        
    base_domain = urlparse(website_url).netloc
    
    # Common paths to check
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
    
    # Keep track of visited URLs IN THIS RUN to avoid infinite loops within the domain
    visited = set()
    
    for path in paths_to_check:
        target_url = urljoin(website_url, path)
        if target_url in visited: continue
        visited.add(target_url)
        
        try:
            # Small timeout so we don't hang on dead sites
            response = requests.get(target_url, headers=headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                links = extract_links_from_page(target_url, soup, base_domain)
                all_discovered.extend(links)
        except requests.exceptions.RequestException as e:
            print(f"  -> Error accessing {target_url}: {e}")
            
        time.sleep(0.5) # Be polite
        
    # Deduplicate within this single run
    unique_discovered = {}
    for item in all_discovered:
        unique_discovered[item['url']] = item
        
    new_findings = []
    cursor = conn.cursor()
    
    for item in unique_discovered.values():
        url = item['url']
        
        # Check if already in DB
        cursor.execute("SELECT id FROM scraped_documents WHERE url = ?", (url,))
        existing = cursor.fetchone()
        
        if not existing:
            try:
                cursor.execute(
                    "INSERT INTO scraped_documents (fund_id, url, title, doc_type) VALUES (?, ?, ?, ?)",
                    (fund_id, url, item['title'], item['doc_type'])
                )
                conn.commit()
                new_findings.append(item)
            except sqlite3.IntegrityError:
                # Caught an edge case uniqueness constraint
                pass
                
    return new_findings

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
    
    print(f"Starting generalized scan of {len(funds)} pension fund websites...")
    
    total_new_docs = 0
    total_new_news = 0
    
    for fund in funds:
        try:
            new_items = scan_fund(fund['id'], fund['name'], fund['website'], conn)
            
            if new_items:
                if args.test:
                    print(f"  *** FOUND {len(new_items)} ITEMS for {fund['name']}! ***")
                for item in new_items:
                    if args.test:
                        print(f"    - [{item['doc_type'].upper()}] {item['title'][:60]}: {item['url']}")
                    if item['doc_type'] == 'document': total_new_docs += 1
                    elif item['doc_type'] == 'news': total_new_news += 1
        except Exception as e:
            print(f"Failed to scan {fund['name']}: {e}")
            
    print("\n========================================================")
    print("BACKLOG INITIALIZATION COMPLETE.")
    print(f"Total New Documents Cataloged: {total_new_docs}")
    print(f"Total New News Articles Cataloged: {total_new_news}")
    print("========================================================")
    
    conn.close()

if __name__ == "__main__":
    main()
