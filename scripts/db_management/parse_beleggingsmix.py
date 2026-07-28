import urllib.request
import ssl
import PyPDF2
from io import BytesIO

ssl._create_default_https_context = ssl._create_unverified_context
url = "https://www.abp.nl/content/dam/abp/documenten/beleggen/dvb-beleid-abp-2024.pdf"
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
print("Downloading ABP DVB Policy 2024 PDF...")
try:
    with urllib.request.urlopen(req) as response:
        pdf_file = BytesIO(response.read())
        reader = PyPDF2.PdfReader(pdf_file)
        
        print(f"Total pages: {len(reader.pages)}")
        
        # Scrape all pages and grep for key terms
        extracted = []
        for p in range(len(reader.pages)):
            try:
                page = reader.pages[p]
                text = page.extract_text()
                if text:
                    lines = text.split('\n')
                    for i, line in enumerate(lines):
                        low = line.lower()
                        if "beleggingsmix" in low or "allocatie" in low or "aandelen" in low or "aandeel" in low or "vastrentend" in low or "obligaties" in low or "%" in low:
                            # Let's collect context around lines with percentages and investment keywords
                            if "%" in low and ("aandelen" in low or "obligaties" in low or "vastgoed" in low or "mix" in low):
                                print(f"[PAGE {p+1}] {line}")
            except Exception as e:
                pass
except Exception as e:
    print(f"Error: {e}")
