import urllib.request
import ssl
import PyPDF2
from io import BytesIO

ssl._create_default_https_context = ssl._create_unverified_context

url = "https://jaarverslag.abp.nl/abp-jaarverslag-2024.pdf"
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
try:
    with urllib.request.urlopen(req) as response:
        pdf_file = BytesIO(response.read())
        reader = PyPDF2.PdfReader(pdf_file)
        # Page 6 corresponds to index 5 (0-indexed). Let's check page 5, 6 and 7 just in case
        for p in range(4, 8):
            try:
                page = reader.pages[p]
                text = page.extract_text()
                print(f"--- PAGE {p+1} ---")
                print(text) 
                print("-" * 20)
            except Exception as e:
                pass
except Exception as e:
    print("Error:", e)
