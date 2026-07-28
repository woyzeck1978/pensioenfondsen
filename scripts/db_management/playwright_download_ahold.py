import urllib.request
from playwright.sync_api import sync_playwright
import PyPDF2
from io import BytesIO

url = "https://www.aholddelhaizepensioen.nl/-/media/Files/Aholddelhaize/adp-jaarverslag-2024.pdf"
pdf_path = "data/reports/73_Ahold_Delhaize.pdf"

print("Downloading Ahold Delhaize 2024 Annual Report via Playwright...")
try:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        print(f"Waiting for download at {url}")
        with page.expect_download(timeout=60000) as download_info:
            page.goto(url)
        
        download = download_info.value
        download.save_as(pdf_path)
        print(f"Downloaded and saved to {pdf_path}")
        
        with open(pdf_path, 'rb') as f:
            pdf_file = BytesIO(f.read())
            
        reader = PyPDF2.PdfReader(pdf_file)
        
        num_pages = len(reader.pages)
        print(f"Total pages: {num_pages}")
        
        print("\\n--- EXTRACTING KEY FIGURES (First 15 Pages) ---")
        for p in range(0, min(15, num_pages)):
            try:
                text = reader.pages[p].extract_text()
                if text:
                    print(f"\\n--- PAGE {p+1} ---\\n")
                    print(text)
            except Exception as e:
                pass
                
        print("\\n--- EXTRACTING SFDR (Last 30 Pages) ---")
        start_sfdr = max(0, num_pages - 30)
        for p in range(start_sfdr, num_pages):
            try:
                text = reader.pages[p].extract_text()
                if text:
                    for line in text.split('\\n'):
                        line_lower = line.lower()
                        if "artikel 8" in line_lower or "artikel 9" in line_lower or ("taxonomie" in line_lower and "%" in line):
                            print(f"[PAGE {p+1}] Match: {line}")
            except Exception as e:
                pass
except Exception as e:
    print(f"Error: {e}")
