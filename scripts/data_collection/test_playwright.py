from playwright.sync_api import sync_playwright

url = "https://www.fysiopensioen.nl/over-ons/bestuur-en-verantwoordingsorgaan"
with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()
    page.goto(url)
    text = page.locator('main').inner_text() if page.locator('main').count() > 0 else page.inner_text('body')
    print("--- Extracted Text ---")
    print(text[:1000])
    browser.close()
