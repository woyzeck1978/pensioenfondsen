import asyncio
from playwright.async_api import async_playwright
import urllib.request
import os

async def download_playwright(url, output_path):
    print(f"Downloading (Playwright): {url}")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
            accept_downloads=True
        )
        page = await context.new_page()
        try:
            async with page.expect_download() as download_info:
                # navigating directly to PDF might trigger download, or we evaluate a click
                await page.evaluate(f"window.location.href = '{url}'")
            download = await download_info.value
            await download.save_as(output_path)
            print(f"Saved to {output_path}")
        except Exception as e:
            print(f"Error downloading {url}: {e}")
            # Try fetching with requests via page.evaluate
            try:
                content = await page.evaluate(f'''async () => {{
                    const resp = await fetch('{url}');
                    const buffer = await resp.arrayBuffer();
                    return Array.from(new Uint8Array(buffer));
                }}()''')
                with open(output_path, 'wb') as f:
                    f.write(bytes(content))
                print(f"Saved via fetch to {output_path}")
            except Exception as e2:
                print(f"Fallback fetch failed: {e2}")
        finally:
            await browser.close()

async def main():
    sbz_url = "https://www.sbzpensioen.nl/-/media/Files/SBZ/Jaarverslagen/SBZ-jaarverslag-2024.pdf"
    zuivel_url = "https://www.pensioenfondszuivel.nl/media/omyntzjv/20250603-bpz-jaarverslag-2024-definitief.pdf"
    
    await download_playwright(sbz_url, "data/processed/SBZ-jaarverslag-2024.pdf")
    await download_playwright(zuivel_url, "data/processed/bpz-jaarverslag-2024.pdf")

asyncio.run(main())
