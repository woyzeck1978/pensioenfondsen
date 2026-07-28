import urllib.request
import ssl
import PyPDF2
from io import BytesIO
import requests
import time

url = "https://www.pensioenfondsachmea.nl/-/media/Files/Achmea/Pensioen-123-laag-3-algemeen/Pensioenfonds-Achmea-Jaarverslag-2024.pdf"
print("Downloading Achmea 2024 Annual Report PDF with requests...")
try:
    s = requests.Session()
    s.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    response = s.get(url, verify=False, timeout=30)
    response.raise_for_status()
    
    pdf_file = BytesIO(response.content)
    reader = PyPDF2.PdfReader(pdf_file)
    
    print(f"Total pages: {len(reader.pages)}")
    
    for p in range(140, 146): # Pages 141-146
        try:
            page = reader.pages[p]
            text = page.extract_text()
            print(f"\\n--- PAGE {p+1} ---\\n")
            print(text)
        except Exception as e:
            pass
except Exception as e:
    print(f"Error: {e}")
