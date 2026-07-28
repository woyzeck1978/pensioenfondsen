import urllib.request
import ssl
import PyPDF2
from io import BytesIO

ssl._create_default_https_context = ssl._create_unverified_context
url = "https://jaarverslag.abp.nl/abp-jaarverslag-2024.pdf"
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
print("Downloading ABP 2024 PDF...")
try:
    with urllib.request.urlopen(req) as response:
        pdf_file = BytesIO(response.read())
        reader = PyPDF2.PdfReader(pdf_file)
        
        # Paginanummer 178 is waarschijnlijk index 177 of daaromheen.
        start_page = 177
        end_page = 181
        total = len(reader.pages)
        print(f"Total pages: {total}")
        for p in range(start_page, min(end_page, total)):
            try:
                page = reader.pages[p]
                text = page.extract_text()
                print(f"--- PAGE {p+1} ---")
                print(text)
                print("-" * 20)
            except Exception as e:
                print(f"Error on page {p+1}: {e}")
except Exception as e:
    print(f"Error: {e}")
