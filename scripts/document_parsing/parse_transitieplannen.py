import requests
import os
import re
import urllib3
from pypdf import PdfReader
from io import BytesIO

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

urls = {
    "APF - AkzoNobel": "https://www.pensioenfondsapf.nl/-/media/Files/Akzo/nieuwe-pensioenregeling/transitieplannen-nl/Transitieplan-AkzoNobel-28-10-2024.pdf",
    "APF - Nouryon": "https://www.pensioenfondsapf.nl/-/media/Files/Akzo/nieuwe-pensioenregeling/transitieplannen-nl/Transitieplan-Nouryon-28-10-2024.pdf",
    "APF - Nobian": "https://www.pensioenfondsapf.nl/-/media/Files/Akzo/nieuwe-pensioenregeling/transitieplannen-nl/Transitieplan-Nobian-28-10-2024.pdf",
    "APF - Salt Specialties": "https://www.pensioenfondsapf.nl/-/media/Files/Akzo/nieuwe-pensioenregeling/transitieplannen-nl/Transitieplan-Salt-Specialties-28-10-2024.pdf"
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

for name, url in urls.items():
    print(f"\n======================== {name} ========================")
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=20)
        response.raise_for_status()
        pdf = PdfReader(BytesIO(response.content))
        
        found_data = []
        for i, page in enumerate(pdf.pages):
            if i > 40: break
            text = page.extract_text()
            if not text: continue
            
            lines = text.split('\n')
            for j, line in enumerate(lines):
                lower_line = line.lower()
                # Search for specific WTP ratios
                if ('dekkingsgraad' in lower_line or 'invaren' in lower_line) and '%' in line:
                    if re.search(r'\d{1,3}(?:,\d{1,2})?\s*%', line):
                        found_data.append(f"Pg {i+1}: {line.strip()}")
        
        for param in found_data[:15]:
            print(param)
        if len(found_data) > 15:
            print(f"... and {len(found_data) - 15} more matches.")
            
    except Exception as e:
        print(f"Failed to fetch or parse {name}: {e}")

