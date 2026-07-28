import urllib.request
import ssl
import PyPDF2
from io import BytesIO
import re

ssl._create_default_https_context = ssl._create_unverified_context
url = "https://jaarverslag.abp.nl/abp-jaarverslag-2024.pdf"
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
try:
    with urllib.request.urlopen(req) as response:
        pdf_file = BytesIO(response.read())
        reader = PyPDF2.PdfReader(pdf_file)
        
        for p in range(176, 185): # SFDR annex usually located here
            try:
                page = reader.pages[p]
                text = page.extract_text()
                if text:
                    for line in text.split('\n'):
                        line_lower = line.lower()
                        if "artikel 8" in line_lower:
                            print(f"[PAGE {p+1}] SFDR Article 8 match: {line}")
                        if "artikel 9" in line_lower:
                            print(f"[PAGE {p+1}] SFDR Article 9 match: {line}")
                        if "taxonomie" in line_lower and "%" in line:
                            print(f"[PAGE {p+1}] Taxonomy match: {line}")
            except Exception as e:
                pass
except Exception as e:
    print(f"Error: {e}")
