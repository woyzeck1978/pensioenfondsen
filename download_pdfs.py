import asyncio
from playwright.async_api import async_playwright
import urllib.request
import os

MIN_PDF_BYTES = 20_000  # een echt jaarverslag is honderden kB; 206 bytes is een 403-pagina


def _keur_pdf(pad):
    """True als er een plausibele PDF staat; anders opruimen en melden.

    Zonder deze controle schrijft de fetch-fallback hieronder gewoon weg wat de
    server teruggaf — en dat was bij vijf fondsen een 'Access Denied'-pagina van
    één pagina, die daarna jarenlang als jaarverslag in data/ stond.
    """
    try:
        with open(pad, "rb") as f:
            kop = f.read(4)
        grootte = os.path.getsize(pad)
    except OSError as e:
        print(f"  Kan {pad} niet lezen: {e}")
        return False
    if kop != b"%PDF":
        print(f"  GEEN PDF (begint met {kop!r}) — verwijderd: {pad}")
        os.remove(pad)
        return False
    if grootte < MIN_PDF_BYTES:
        print(f"  Verdacht klein ({grootte} bytes) — verwijderd: {pad}")
        os.remove(pad)
        return False
    return True


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
            if _keur_pdf(output_path):
                print(f"Saved to {output_path}")
                return
            # Geen bruikbare PDF: doorschakelen naar de fetch-fallback hieronder.
            raise RuntimeError("download leverde geen bruikbare PDF op")
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
                if _keur_pdf(output_path):
                    print(f"Saved via fetch to {output_path}")
                else:
                    print(f"Fetch gaf geen PDF terug voor {url}")
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
