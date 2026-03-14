import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()
        print("Navigating to BPL Pensioen...")
        await page.goto("https://www.bplpensioen.nl/publicaties-en-documenten", wait_until="networkidle")
        
        # Give it a second
        await page.wait_for_timeout(2000)
        
        # Try to find 'jaarverslag' links
        links = await page.evaluate('''() => {
            const anchors = Array.from(document.querySelectorAll('a'));
            return anchors
                .filter(a => a.href && a.href.toLowerCase().includes('.pdf') && (a.href.toLowerCase().includes('jaarverslag') || a.textContent.toLowerCase().includes('jaarverslag')))
                .map(a => a.href);
        }''')
        
        print(f"BPL Jaarverslag Links found: {links}")
        await browser.close()

asyncio.run(run())
