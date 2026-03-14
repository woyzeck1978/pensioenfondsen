from playwright.sync_api import sync_playwright

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto('https://bpfl.nl/documenten', timeout=60000)
        
        links = page.locator('a').all()
        target_url = None
        for link in links:
            href = link.get_attribute('href')
            if href and '.pdf' in href.lower() and 'jaarverslag' in href.lower():
                target_url = href if href.startswith('http') else 'https://bpfl.nl' + href
                break
        
        if target_url:
            print(f"Downloading BPFL from: {target_url}")
            response = page.request.get(target_url)
            with open('data/processed/bpfl-jaarverslag.pdf', 'wb') as f:
                f.write(response.body())
        else:
            print("No BPFL jaarverslag PDF found.")
            
        browser.close()

if __name__ == '__main__':
    run()
