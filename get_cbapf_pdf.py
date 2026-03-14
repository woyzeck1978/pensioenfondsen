from playwright.sync_api import sync_playwright

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        pdf_url = 'https://www.centraalbeheerapf.nl/-/media/Files/CentraalBeheer/Jaarverslagen/cb-apf-jaarverslag-2024.pdf'
        
        # We can try to get the pdf as a buffer
        response = page.request.get(pdf_url)
        with open('data/processed/cb-apf-2024.pdf', 'wb') as f:
            f.write(response.body())
        browser.close()

if __name__ == '__main__':
    run()
