from playwright.sync_api import sync_playwright

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        print("Navigating to Achmea publications page...")
        try:
            page.goto("https://www.pensioenfondsachmea.nl/publicaties/jaarverslagen", timeout=60000)
            page.wait_for_load_state("networkidle")
            
            links = page.locator("a").element_handles()
            pdf_links = []
            for link in links:
                href = link.get_attribute("href")
                if href and ".pdf" in href.lower():
                    text = link.inner_text().strip()
                    pdf_links.append(f"{text}: {href}")
            
            print("Found PDF links:")
            for l in pdf_links:
                print(l)
        except Exception as e:
            print(f"Error: {e}")
            print("Page content snippet:")
            print(page.content()[:1000])
        finally:
            browser.close()

if __name__ == "__main__":
    run()
