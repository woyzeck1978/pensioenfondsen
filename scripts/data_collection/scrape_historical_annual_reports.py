import sqlite3
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import os
import urllib3
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
DB_PATH = 'data/processed/pension_funds.db'
OUTPUT_DIR = 'data/historical_reports/'
MAX_WORKERS = 10
TIMEOUT = 15

# Common subpaths to hunt for reports
SUBPATHS_TO_CHECK = [
    '',
    '/over-ons',
    '/documenten',
    '/downloads',
    '/jaarverslagen',
    '/organisatie',
    '/pensioenfonds',
    '/over-het-pensioenfonds',
    '/bibliotheek',
    '/publicaties'
]

def load_funds():
    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT id, name, website 
    FROM funds 
    WHERE status NOT LIKE '%Opgeheven%' 
    AND status NOT LIKE '%Liquidatie%' 
    AND status != 'Overdracht'
    AND website IS NOT NULL
    AND website != ''
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def download_file(url, filepath):
    if os.path.exists(filepath):
        return False, "Already exists"
    
    try:
        response = requests.get(url, verify=False, timeout=TIMEOUT, headers={'User-Agent': 'Mozilla/5.0'})
        if response.status_code == 200 and 'application/pdf' in response.headers.get('Content-Type', '').lower():
            with open(filepath, 'wb') as f:
                f.write(response.content)
            return True, "Downloaded"
        return False, f"Not a PDF / Status: {response.status_code}"
    except Exception as e:
        return False, f"Error: {e}"

def find_reports_on_page(url, soup, base_url):
    reports = []
    links = soup.find_all('a', href=True)
    
    for link in links:
        href = link['href']
        text = link.text.lower()
        
        # Check if it's a PDF link and sounds like an annual report
        is_pdf = '.pdf' in href.lower()
        is_report = 'jaarverslag' in href.lower() or 'verslag' in text or 'annual report' in text or 'jaarverslag' in text
        
        if is_pdf and is_report:
            full_url = urljoin(base_url, href)
            # Try to extract a year (2010 to 2029)
            year_match = re.search(r'(20[1-2]\d)', href + text)
            if year_match:
                year = year_match.group(1)
                reports.append((year, full_url))
                
    return reports

def scrape_fund(fund_id, fund_name, website):
    base_url = website.rstrip('/')
    domain = urlparse(base_url).netloc
    
    found_reports = {} # year -> url mapping to avoid duplicates for the same year
    
    # Try different common subpages
    for subpath in SUBPATHS_TO_CHECK:
        target_url = base_url + subpath
        try:
            response = requests.get(target_url, verify=False, timeout=TIMEOUT, headers={'User-Agent': 'Mozilla/5.0'})
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                reports = find_reports_on_page(target_url, soup, base_url)
                
                for year, pdf_url in reports:
                    # Ignore 2024 as we already extracted those manually/separately, but collect anything else
                    if year not in found_reports and year != '2024':
                        found_reports[year] = pdf_url
        except Exception:
            pass # Keep trying other subpaths even if one gets a 404/timeout
        
        # small delay to prevent rate-limiting when crawling the same domain
        time.sleep(0.5)
        
    results = []
    for year, pdf_url in found_reports.items():
        # Sanitize fund name for filename
        clean_name = re.sub(r'[^a-zA-Z0-9\s]', '', fund_name).strip()
        filename = f"{fund_id}_{clean_name}_{year}.pdf"
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        success, msg = download_file(pdf_url, filepath)
        results.append({
            'fund_id': fund_id,
            'fund_name': fund_name,
            'year': year,
            'url': pdf_url,
            'status': msg
        })
        
    return results

def main():
    print("Starting historical annual reports scraper...")
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    funds_df = load_funds()
    total_funds = len(funds_df)
    print(f"Loaded {total_funds} active funds to scan.")
    
    all_results = []
    downloaded_files = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all scraping tasks
        future_to_fund = {
            executor.submit(scrape_fund, row['id'], row['name'], row['website']): row['name'] 
            for _, row in funds_df.iterrows()
        }
        
        for i, future in enumerate(as_completed(future_to_fund), 1):
            fund_name = future_to_fund[future]
            try:
                results = future.result()
                all_results.extend(results)
                success_count = sum(1 for r in results if r['status'] == 'Downloaded')
                downloaded_files += success_count
                if len(results) > 0:
                    print(f"[{i}/{total_funds}] {fund_name}: Found {len(results)} reports ({success_count} downloaded)")
                else:
                    print(f"[{i}/{total_funds}] {fund_name}: Found 0 older reports.")
            except Exception as e:
                print(f"[{i}/{total_funds}] {fund_name}: Scrape failed - {e}")

    print("-" * 50)
    print(f"Scraping complete! Total historical files newly downloaded: {downloaded_files}")
    
    # Optional: Save a log of what was found
    if all_results:
        log_df = pd.DataFrame(all_results)
        log_df.to_csv(os.path.join(OUTPUT_DIR, 'scrape_log.csv'), index=False)
        print("Detailed log saved to data/historical_reports/scrape_log.csv")

if __name__ == '__main__':
    main()
