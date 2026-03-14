import sqlite3
import os
import requests
import time
from bs4 import BeautifulSoup
import urllib.parse
import urllib3
import re
import argparse

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_missing_report_funds(db_path, limit=None):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = "SELECT id, name, website FROM funds WHERE annual_report_downloaded = 0"
    if limit:
        query += f" LIMIT {limit}"
    cursor.execute(query)
    funds = cursor.fetchall()
    conn.close()
    return funds

def mark_downloaded(db_path, fund_id):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("UPDATE funds SET annual_report_downloaded = 1 WHERE id = ?", (fund_id,))
    conn.commit()
    conn.close()

def download_pdf(url, filepath):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        response = requests.get(url, headers=headers, timeout=20, stream=True, verify=False)
        response.raise_for_status()
        
        content_type = response.headers.get('content-type', '').lower()
        if 'text/html' in content_type:
            print(f"    Skipping: URL is an HTML page: {content_type}")
            return False
            
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        # Check if it actually looks like a PDF inside
        with open(filepath, 'rb') as f:
            header = f.read(5)
            if header != b'%PDF-':
                print(f"    Error: Downloaded file is not a valid PDF (Header: {header})")
                os.remove(filepath)
                return False
                
        if os.path.getsize(filepath) < 50 * 1024:
            print("    Error: PDF is suspiciously small (< 50KB), likely a placeholder or error page.")
            os.remove(filepath)
            return False
            
        return True
    except Exception as e:
        print(f"    Failed download {url}: {e}")
        return False

def deep_search_report(website):
    if not website:
        return None
        
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    
    domain = urllib.parse.urlparse(website).netloc
    
    visited = set()
    to_visit = [website]
    
    # Prioritized terms that suggest a page might contain the report
    priority_keywords = ['document', 'download', 'publicatie', 'verantwoording', 'over-ons', 'bibliotheek', 'archief', 'jaarverslag', 'organisatie']
    
    # Terms that strongly suggest a link is the actual PDF we want
    pdf_text_keywords = ['jaarverslag', 'jaarrapport', 'annual report', 'annual-report', 'verkort jaarverslag']
    valid_years = ['2023', '2024', '2022', '2021']
    
    def is_pdf_link(href, text):
        href_lower = href.lower()
        text_lower = text.lower()
        
        # Must be a PDF
        if not (href_lower.endswith('.pdf') or '.pdf?' in href_lower or '/pdf/' in href_lower):
            return False
            
        # If the URL or the link text contains a strong keyword
        has_keyword = any(k in href_lower or k in text_lower for k in pdf_text_keywords)
        if not has_keyword:
            return False
            
        # Ideally, it should contain a recent year, or the word 'jaarverslag' outright with no other junk
        has_year = any(y in href_lower or y in text_lower for y in valid_years)
        
        if has_keyword and has_year:
            return True
            
        # Fallback if it literally just says "jaarverslag.pdf" with no year
        if 'jaarverslag' in href_lower and ('2019' not in href_lower and '2018' not in href_lower and '2020' not in href_lower):
            return True
            
        return False

    pages_crawled = 0
    max_pages = 25 # Increased threshold to dig deeper into the site
    
    while to_visit and pages_crawled < max_pages:
        url = to_visit.pop(0)
        if url in visited: continue
        visited.add(url)
        pages_crawled += 1
        
        try:
            # print(f"    Crawling: {url}")
            # Try to get the page
            req = requests.get(url, headers=headers, timeout=10, verify=False)
            if req.status_code != 200:
                continue
                
            # If the URL itself redirects directly to a PDF
            if req.headers.get('content-type', '').lower() == 'application/pdf':
                 if is_pdf_link(url, ""):
                     return url
            
            soup = BeautifulSoup(req.text, 'html.parser')
            
            # Step 1: Look for the holy grail PDF link on this page
            for a in soup.find_all('a', href=True):
                href = urllib.parse.urljoin(url, a['href'])
                text = a.get_text().strip()
                
                if is_pdf_link(href, text):
                    print(f"    Found deep URL on page {pages_crawled}: {href}")
                    return href
            
            # Step 2: Extract more links to continue crawling
            # Find all internal links
            new_links = []
            for a in soup.find_all('a', href=True):
                href = urllib.parse.urljoin(url, a['href'])
                href_parsed = urllib.parse.urlparse(href)
                
                # Only follow internal links, ignoring fragments/anchors
                if href_parsed.netloc == domain:
                    clean_href = href.split('#')[0]
                    
                    if clean_href not in visited and clean_href not in to_visit:
                        # Avoid crawling obviously useless directories
                        if any(bad in clean_href.lower() for bad in ['/nieuws/', '/contact/', '/faq/', '/veelgestelde-vragen/']):
                            continue
                            
                        # If the URL string contains priority keywords, sort it to the front
                        is_priority = any(k in clean_href.lower() or k in a.get_text().lower() for k in priority_keywords)
                        if is_priority:
                            to_visit.insert(0, clean_href) # Prepend
                        else:
                            to_visit.append(clean_href) # Append
                            
        except requests.exceptions.Timeout:
            print(f"    Timeout on {url}")
        except Exception as e:
            # print(f"    Error on {url}: {e}")
            pass
            
        time.sleep(1) # Be nice to the pension fund's server
        
    return None

def main():
    parser = argparse.ArgumentParser(description="Advanced deep crawl for missing annual reports")
    parser.add_argument("--limit", type=int, help="Limit number of funds to process")
    args = parser.parse_args()

    db_path = '../../data/processed/pension_funds.db'
    output_dir = '../data/reports'

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    funds = get_missing_report_funds(db_path, limit=args.limit)
    print(f"Found {len(funds)} funds still missing their annual report.")

    success_count = 0
    for i, (fund_id, fund_name, website) in enumerate(funds):
        print(f"\n[{i+1}/{len(funds)}] Deep crawling [{fund_id}] {fund_name}...")
        safe_name = "".join(x for x in fund_name if x.isalnum() or x in " -_").strip()
        filepath = os.path.join(output_dir, f"{fund_id}_{safe_name}.pdf")

        if os.path.exists(filepath):
            print(f"  File already exists locally: {filepath}")
            mark_downloaded(db_path, fund_id)
            success_count += 1
            continue

        url = deep_search_report(website)
        
        if url:
            if download_pdf(url, filepath):
                print(f"  => Successfully downloaded deep-linked PDF to {filepath}")
                mark_downloaded(db_path, fund_id)
                success_count += 1
            else:
                print("  => Download failed or invalid PDF.")
        else:
            print("  => No suitable PDF found even after deep crawl.")
        
    print(f"\nDone! Deep crawler successfully harvested {success_count}/{len(funds)} extra reports.")

if __name__ == "__main__":
    main()
