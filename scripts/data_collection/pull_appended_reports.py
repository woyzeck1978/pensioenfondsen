import sqlite3
import os
import requests
import time
import urllib3
from bs4 import BeautifulSoup
import urllib.parse
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_missing_report_funds(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # ONLY GET THE APPENDED DNB FUNDS that haven't been downloaded yet
    query = "SELECT id, name, website FROM funds WHERE data_source = 'DNB Appendix' AND annual_report_downloaded != 1 AND website IS NOT NULL"
    cursor.execute(query)
    funds = cursor.fetchall()
    conn.close()
    return funds

def search_annual_report(fund_name, website):
    if not website: return None
        
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/114.0.0.0 Safari/537.36'}
    
    def find_report_in_soup(soup, base_url):
        for a in soup.find_all('a', href=True):
            text = a.get_text().lower()
            href = a['href']
            if 'jaarverslag' in text or 'annual report' in text or 'jaarbericht' in text or 'jaarrapport' in text:
                if '.pdf' in href.lower():
                    return urllib.parse.urljoin(base_url, href)
            if ('jaarverslag' in href.lower() or 'annual-report' in href.lower()) and '.pdf' in href.lower():
                return urllib.parse.urljoin(base_url, href)
        return None

    try:
        print(f"  Crawling homepage: {website}")
        req = requests.get(website, headers=headers, timeout=12, verify=False)
        base_url = req.url
        soup = BeautifulSoup(req.text, 'html.parser')
        
        pdf_url = find_report_in_soup(soup, base_url)
        if pdf_url: return pdf_url
        
        docs_url = None
        for a in soup.find_all('a', href=True):
            text = a.get_text().lower()
            if 'document' in text or 'download' in text or 'publicatie' in text or 'verantwoording' in text or 'over-ons' in text:
                docs_url = urllib.parse.urljoin(base_url, a['href'])
                break
                
        if docs_url:
            print(f"  Crawling subpage: {docs_url}")
            req2 = requests.get(docs_url, headers=headers, timeout=12, verify=False)
            soup2 = BeautifulSoup(req2.text, 'html.parser')
            pdf_url = find_report_in_soup(soup2, req2.url)
            if pdf_url: return pdf_url
            
    except Exception as e:
        print(f"  Error crawling {website}: {e}")
    return None

def download_pdf(url, filepath):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36'}
        response = requests.get(url, headers=headers, timeout=20, stream=True, verify=False)
        response.raise_for_status()
        
        content_type = response.headers.get('content-type', '').lower()
        if 'text/html' in content_type: return False
            
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        with open(filepath, 'rb') as f:
            header = f.read(5)
            if header != b'%PDF-':
                os.remove(filepath)
                return False
                
        if os.path.getsize(filepath) < 50 * 1024:
            os.remove(filepath)
            return False
            
        return True
    except Exception as e:
        return False

def mark_downloaded(db_path, fund_id):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("UPDATE funds SET annual_report_downloaded = 1 WHERE id = ?", (fund_id,))
    conn.commit()
    conn.close()

def main():
    db_path = '../../data/processed/pension_funds.db'
    output_dir = '../data/reports'
    if not os.path.exists(output_dir): os.makedirs(output_dir)

    funds = get_missing_report_funds(db_path)
    print(f"Found {len(funds)} DNB Appended funds needing a report download.")

    success_count = 0
    for i, (fund_id, fund_name, website) in enumerate(funds):
        print(f"\n[{i+1}/{len(funds)}] Processing [{fund_id}] {fund_name}...")
        safe_name = "".join(x for x in fund_name if x.isalnum() or x in " -_").strip()
        filename = f"{fund_id}_{safe_name}.pdf"
        filepath = os.path.join(output_dir, filename)

        if os.path.exists(filepath):
            print(f"  File already exists: {filepath}")
            mark_downloaded(db_path, fund_id)
            success_count += 1
            continue

        url = search_annual_report(fund_name, website)
        if url:
            print(f"  Found URL: {url}")
            if download_pdf(url, filepath):
                print(f"  => Successfully downloaded to {filepath}")
                mark_downloaded(db_path, fund_id)
                success_count += 1
            else:
                print("  => Download failed or invalid PDF.")
        else:
            print("  => No suitable PDF found.")
        
        time.sleep(3)

    print(f"\nDone! Successfully downloaded {success_count}/{len(funds)} reports.")

if __name__ == "__main__":
    main()
