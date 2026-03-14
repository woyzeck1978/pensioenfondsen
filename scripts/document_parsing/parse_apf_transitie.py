import requests
import os
import re
from pypdf import PdfReader
from io import BytesIO

urls = {
    "AkzoNobel": "https://www.pensioenfondsapf.nl/-/media/Files/Akzo/nieuwe-pensioenregeling/transitieplannen-nl/Transitieplan-AkzoNobel-28-10-2024.pdf",
    "Nouryon": "https://www.pensioenfondsapf.nl/-/media/Files/Akzo/nieuwe-pensioenregeling/transitieplannen-nl/Transitieplan-Nouryon-28-10-2024.pdf",
    "Nobian": "https://www.pensioenfondsapf.nl/-/media/Files/Akzo/nieuwe-pensioenregeling/transitieplannen-nl/Transitieplan-Nobian-28-10-2024.pdf",
    "Salt Specialties": "https://www.pensioenfondsapf.nl/-/media/Files/Akzo/nieuwe-pensioenregeling/transitieplannen-nl/Transitieplan-Salt-Specialties-28-10-2024.pdf"
}

headers = {'User-Agent': 'Mozilla/5.0'}

for kring, url in urls.items():
    print(f"\n======================== {kring} ========================")
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=15)
        response.raise_for_status()
        pdf = PdfReader(BytesIO(response.content))
        
        # We want to find Dkkingsgraad/Funding ratio limits and indexatie/toeslag verlening
        for i, page in enumerate(pdf.pages):
            if i > 30: break
            text = page.extract_text()
            if not text: continue
            
            lines = text.split('\n')
            for j, line in enumerate(lines):
                # Search for keyword "dekkingsgraad" or "indexatie" with a percentage
                if ('dekkingsgraad' in line.lower() or 'indexatie' in line.lower() or 'toeslag' in line.lower() or 'verhoging' in line.lower()) and '%' in line:
                    if re.search(r'\d{1,3}(?:,\d{1,2})?\s*%', line):
                        print(line.strip())
    
    except Exception as e:
        print(f"Failed to fetch or parse: {e}")

