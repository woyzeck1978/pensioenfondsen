import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import urllib3
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")
MAX_WORKERS = 10
TIMEOUT = 15

# Common subpaths to hunt for reports
SUBPATHS_TO_CHECK = [
    '', '/over-ons', '/documenten', '/downloads', '/jaarverslagen', 
    '/organisatie', '/pensioenfonds', '/over-het-pensioenfonds', 
    '/bibliotheek', '/publicaties', '/esg', '/duurzaamheid', '/verantwoord-beleggen',
    '/beleggingen'
]

def load_missing_funds():
    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT f.id, f.name, f.website 
    FROM funds f 
    LEFT JOIN (
        SELECT DISTINCT fund_id 
        FROM scraped_documents 
        WHERE doc_type = 'document' 
          AND (lower(title) LIKE '%duurzaam%' 
               OR lower(title) LIKE '%esg%' 
               OR lower(title) LIKE '%maatschappelijk%' 
               OR lower(title) LIKE '%mvo%')
    ) esg ON f.id = esg.fund_id
    WHERE f.status = 'Open' 
      AND esg.fund_id IS NULL
      AND f.website IS NOT NULL 
      AND f.website != ''
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def find_esg_reports_on_page(url, soup, base_url):
    reports = []
    links = soup.find_all('a', href=True)
    
    for link in links:
        href = link['href']
        text = link.text.lower()
        full_url = urljoin(base_url, href)
        
        is_pdf = '.pdf' in href.lower()
        is_esg = ('duurzaam' in href.lower() or 'esg' in href.lower() or 'maatschappelijk' in href.lower() or 'mvo' in href.lower() or
                  'duurzaam' in text or 'esg' in text or 'maatschappelijk' in text or 'mvo' in text)
        is_report = 'verslag' in href.lower() or 'rapport' in href.lower() or 'verslag' in text or 'rapport' in text
        
        if is_pdf and is_esg and is_report:
            title = text.strip().title() if text.strip() else "ESG/Duurzaamheidsverslag"
            reports.append((title, full_url))
            
    return reports

def scrape_fund_esg(fund_id, fund_name, website):
    base_url = website.rstrip('/')
    found_reports = {} # url -> title
    
    for subpath in SUBPATHS_TO_CHECK:
        target_url = base_url + subpath
        try:
            response = requests.get(target_url, verify=False, timeout=TIMEOUT, headers={'User-Agent': 'Mozilla/5.0'})
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                reports = find_esg_reports_on_page(target_url, soup, base_url)
                
                for title, pdf_url in reports:
                    if pdf_url not in found_reports:
                        found_reports[pdf_url] = title
        except Exception:
            pass
            
        time.sleep(0.5)
        
    results = []
    for pdf_url, title in found_reports.items():
        results.append({
            'fund_id': fund_id,
            'url': pdf_url,
            'title': title
        })
    return results

def main():
    print("Starting targeted ESG annual reports scraper via domain crawling...")
    funds_df = load_missing_funds()
    total_funds = len(funds_df)
    print(f"Loaded {total_funds} funds missing an ESG report.")
    
    all_results = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_fund = {
            executor.submit(scrape_fund_esg, row['id'], row['name'], row['website']): row['name'] 
            for _, row in funds_df.iterrows()
        }
        
        for i, future in enumerate(as_completed(future_to_fund), 1):
            fund_name = future_to_fund[future]
            try:
                results = future.result()
                all_results.extend(results)
                if len(results) > 0:
                    print(f"[{i}/{total_funds}] {fund_name}: Found {len(results)} ESG-related PDFs")
                else:
                    print(f"[{i}/{total_funds}] {fund_name}: No ESG PDFs found.")
            except Exception as e:
                print(f"[{i}/{total_funds}] {fund_name}: Scrape failed - {e}")

    # Inject into database
    if all_results:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        inserted = 0
        now = datetime.now()
        
        for res in all_results:
            try:
                cursor.execute("""
                    INSERT INTO scraped_documents (fund_id, url, title, doc_type, discovered_at)
                    VALUES (?, ?, ?, 'document', ?)
                    ON CONFLICT(url) DO UPDATE SET title=excluded.title, discovered_at=excluded.discovered_at
                """, (res['fund_id'], res['url'], res['title'], now))
                inserted += 1
            except Exception as e:
                print(f"DB Error: {e}")
                
        conn.commit()
        conn.close()
        print(f"\\nScraping complete! Total newly discovered and injected ESG reports: {inserted}")
    else:
        print("\\nScraping complete! No new ESG reports found.")

if __name__ == '__main__':
    main()
