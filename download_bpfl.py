import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
            accept_downloads=True
        )
        page = await context.new_page()
        print("Navigating to BPFL documenten...")
        await page.goto("https://bpfl.nl/documenten#Jaarverslagen", wait_until="networkidle")
        
        print("Waiting for Jaarverslagen to load...")
        try:
            # Click the Jaarverslagen accordion just in case
            await page.get_by_text("Jaarverslagen", exact=True).click()
            await page.wait_for_timeout(2000)
        except Exception:
            pass
        
        # Look for the download link
        async with page.expect_download() as download_info:
            await page.get_by_role("link", name="Jaarverslag").first.click()
            
        download = await download_info.value
        path = "data/processed/bpfl_jaarverslag.pdf"
        await download.save_as(path)
        print(f"Downloaded reliably to {path}")
        await browser.close()

asyncio.run(run())
