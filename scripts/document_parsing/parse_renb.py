import requests
from pypdf import PdfReader
from io import BytesIO
import urllib3
import re

urllib3.disable_warnings()

url = "https://www.rijnenbinnenvaartpensioen.nl/media/16898/jv_renb_2024.pdf"
headers = {'User-Agent': 'Mozilla/5.0'}

try:
    response = requests.get(url, headers=headers, verify=False, timeout=30)
    response.raise_for_status()
    pdf = PdfReader(BytesIO(response.content))
    
    print("\n--- Extracting AUM ---")
    
    for i, page in enumerate(pdf.pages):
        if i > 25: break 
        text = page.extract_text()
        if not text: continue
        
        lines = text.split('\n')
        for line in lines:
            lower_line = line.lower()
            if 'vermogen' in lower_line and ('mln' in lower_line or 'miljoen' in lower_line or 'mld' in lower_line):
                print(f"Pg {i+1}: {line.strip()}")
                
except Exception as e:
    print(f"Failed to fetch or parse PDF: {e}")
