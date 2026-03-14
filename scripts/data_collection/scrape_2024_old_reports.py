import os
import sqlite3
import subprocess
import time
import requests

TARGET_IDS = [4, 5, 11, 23, 41, 98, 108, 114, 120, 126]
DB_PATH = "../../data/processed/pension_funds.db"
REPORTS_DIR = "../data/reports"

def search_and_download():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Get names of target funds
    placeholders = ','.join('?' for _ in TARGET_IDS)
    c.execute(f"SELECT id, name FROM funds WHERE id IN ({placeholders})", TARGET_IDS)
    funds = c.fetchall()
    conn.close()

    for fund_id, fund_name in funds:
        print(f"\\n--- Attempting to find 2024 report for: {fund_name} (ID: {fund_id}) ---")
        
        # We will use duckduckgo_search if available, else a raw requests search if possible, or just a dummy URL fallback in this sandboxed environment
        # Because we're in an agent env, let's try a few robust search queries via duckduckgo HTML
        query = f'"{fund_name}" jaarverslag 2024 filetype:pdf'
        print(f"Query: {query}")
        
        try:
            from googlesearch import search
            results = list(search(query, num_results=3))
            
            downloaded = False
            for url in results:
                if url.endswith('.pdf'):
                    print(f"Found PDF URL: {url}")
                    
                    safe_name = fund_name.replace("/", "").replace(" - ", " ").strip()
                    safe_name = "".join([c if c.isalnum() or c == ' ' else '' for c in safe_name])
                    filename = f"{fund_id}_{safe_name}.pdf"
                    filepath = os.path.join(REPORTS_DIR, filename)

                    try:
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
                        }
                        resp = requests.get(url, timeout=15, stream=True, headers=headers, verify=False)
                        if resp.status_code == 200 and 'application/pdf' in resp.headers.get('Content-Type', '').lower():
                            with open(filepath, 'wb') as f:
                                for chunk in resp.iter_content(chunk_size=8192):
                                    f.write(chunk)
                            
                            # Check if it's a real PDF
                            with open(filepath, 'rb') as f:
                                header = f.read(5)
                                if header == b'%PDF-':
                                    print(f"SUCCESS: Downloaded {filename} from Google Search")
                                    downloaded = True
                                    break
                                else:
                                    print("File not a valid PDF. Removing.")
                                    os.remove(filepath)
                    except Exception as e:
                        print(f"Download failed for {url}: {e}")
            
            if not downloaded:
                print("Could not find a direct PDF link.")
                
        except ImportError:
            print("googlesearch not installed, unable to fetch automatically.")

if __name__ == "__main__":
    search_and_download()
