from playwright.sync_api import sync_playwright
import re

TARGETS = [
    {"id": 16, "name": "BPL Pensioen", "url": "https://www.bplpensioen.nl/financiele-situatie"},
    {"id": 6, "name": "SPOA", "url": "https://www.spoa.nl/pensioen-en-financiele-situatie/hoe-staan-we-ervoor"},
    {"id": 64, "name": "APF Het Nederlandse Pensioenfonds", "url": "https://hnpf.nl/deelnemer/financiele-situatie/"},
    {"id": 117, "name": "Nederlandse Bisdommen", "url": "https://www.pnb.nl/bisdom/financiele-situatie"},
    {"id": 137, "name": "Witteveen+Bos", "url": "https://pensioenfonds.witteveenbos.nl/financiele-situatie"},
    {"id": 145, "name": "Nationale-Nederlanden (De Nationale APF)", "url": "https://www.denationaleapf.nl/financiele-situatie/"}
]

def check_funds():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for target in TARGETS:
            print(f"\nScanning {target['name']} ({target['url']})")
            try:
                page.goto(target['url'], timeout=15000)
                page.wait_for_timeout(2000)
                text = page.locator('body').inner_text()
                
                # Simple regex to find numbers near 'dekkingsgraad'
                matches = re.finditer(r'(..{0,40})(dekkingsgraad)(.{0,40})', text, re.IGNORECASE)
                found = False
                for m in matches:
                    snippet = m.group(0).replace('\n', ' ').strip()
                    if '%' in snippet:
                        print(f"  Snippet: {snippet}")
                        found = True
                if not found:
                    print("  -> Could not find dekkingsgraad with % in text.")
            except Exception as e:
                print(f"  -> Error: {e}")

        browser.close()

if __name__ == '__main__':
    check_funds()
