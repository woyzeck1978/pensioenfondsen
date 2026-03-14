import os
import re
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright

OUTPUT_DIR = 'data/historical_reports/'

TARGETS = [
    {"fund": "Architectenpensioen", "url": "https://www.architectenpensioen.nl/over-ons/financiele-situatie/jaarverslag"},
    {"fund": "Avebe", "url": "https://www.pensioenfondsavebe.nl/over-ons/brochures-en-formulieren"},
    {"fund": "Foodservice", "url": "https://www.bpffoodservice.nl/documenten/"},
    {"fund": "BPL", "url": "https://www.bplpensioen.nl/jaarverslagen"},
    {"fund": "Bakkersbedrijf", "url": "https://www.bakkerspensioen.nl/jaarverslagen"},
    {"fund": "Betonpensioen", "url": "https://www.betonpensioen.nl/documenten/"},
    {"fund": "Centraal Beheer APF", "url": "https://www.centraalbeheerapf.nl/over-centraal-beheer-apf/jaarverslagen"},
    {"fund": "BPFL", "url": "https://bpfl.nl/documenten"}
]

def download_reports():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for target in TARGETS:
            fund_name = target["fund"]
            url = target["url"]
            print(f"\nScanning {fund_name} at {url}")
            
            try:
                page.goto(url, timeout=30000, wait_until="domcontentloaded")
                
                # Give dynamic pages a moment to load
                page.wait_for_timeout(2000)

                links = page.locator('a').all()
                found_reports = {}

                for link in links:
                    href = link.get_attribute('href')
                    text = link.inner_text().strip() if link.inner_text() else ""
                    
                    if href and '.pdf' in href.lower() and ('jaarverslag' in href.lower() or 'jaarverslag' in text.lower() or 'verslag' in text.lower()):
                        full_url = href if href.startswith('http') else urljoin(url, href)
                        
                        # Look for a year like 2018-2024
                        year_match = re.search(r'(20[1-2]\d)', href + text)
                        if year_match:
                            year = year_match.group(1)
                            if int(year) >= 2018 and int(year) <= 2024:
                                found_reports[year] = full_url

                if not found_reports:
                    print(f"  -> No reports found for {fund_name}.")
                    continue

                for year, pdf_url in found_reports.items():
                    clean_name = re.sub(r'[^a-zA-Z0-9]', '', fund_name)
                    filename = f"MANUAL_{clean_name}_{year}.pdf"
                    filepath = os.path.join(OUTPUT_DIR, filename)

                    if os.path.exists(filepath):
                        print(f"  -> Skipped {year} (already exists)")
                        continue

                    print(f"  -> Downloading {year} from {pdf_url}")
                    try:
                        pdf_resp = page.request.get(pdf_url)
                        if pdf_resp.ok:
                            with open(filepath, 'wb') as f:
                                f.write(pdf_resp.body())
                            print(f"     Saved {filename}")
                        else:
                            print(f"     Failed: HTTP {pdf_resp.status}")
                    except Exception as e:
                        print(f"     Failed to download: {e}")

            except Exception as e:
                print(f"  -> Error scanning page: {e}")

        browser.close()
    print("\nTargeted scraping complete.")

if __name__ == '__main__':
    download_reports()
