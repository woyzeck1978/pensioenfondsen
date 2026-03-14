import requests
from pypdf import PdfReader
from io import BytesIO
import urllib3

urllib3.disable_warnings()

url = "https://www.denationaleapf.nl/media/7875/persbericht-afm.pdf"
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Accept': 'application/pdf'
}

print("Fetching PDF...")
try:
    response = requests.get(url, headers=headers, verify=False, timeout=15)
    response.raise_for_status()
    pdf = PdfReader(BytesIO(response.content))
    
    print("\n--- Extracted Text ---")
    for i, page in enumerate(pdf.pages):
        text = page.extract_text()
        if text:
            print(f"Page {i+1}:\n{text.strip()}\n")
            
except Exception as e:
    print(f"Failed to fetch or parse PDF: {e}")
