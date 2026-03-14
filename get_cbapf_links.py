from playwright.sync_api import sync_playwright

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto('https://www.centraalbeheerapf.nl/over-centraal-beheer-apf/jaarverslagen', timeout=60000)
        
        links = page.locator('a').all()
        for link in links:
            href = link.get_attribute('href')
            if href and '.pdf' in href.lower() and 'jaarverslag' in href.lower():
                url = href if href.startswith('http') else 'https://www.centraalbeheerapf.nl' + href
                print(f"Found PDF: {url}")
        
        browser.close()

if __name__ == '__main__':
    run()
