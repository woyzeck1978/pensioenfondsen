import requests
from pypdf import PdfReader
from io import BytesIO
import urllib3
import re

urllib3.disable_warnings()

targets = {
    "Kring AFM (De Nationale)": "https://www.denationaleapf.nl/media/17369/transitieplan-kring-afm.pdf",
    "Kring ANWB (De Nationale)": "https://www.denationaleapf.nl/media/17225/2025-11-24-transitieplan-anwb-def.pdf",
    "Kring McCain (De Nationale)": "https://www.denationaleapf.nl/media/17081/transitieplan-mccain.pdf"
}

headers = {'User-Agent': 'Mozilla/5.0'}

for name, url in targets.items():
    print(f"\n================ {name} ================")
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=30)
        response.raise_for_status()
        pdf = PdfReader(BytesIO(response.content))
        
        found = False
        for i, page in enumerate(pdf.pages):
            if i > 50: break
            text = page.extract_text()
            if not text: continue
            
            lines = text.split('\n')
            for j, line in enumerate(lines):
                lower_line = line.lower()
                
                if ('invaren' in lower_line or 'dekkingsgraad' in lower_line) and ('%' in line or 'procent' in line):
                    context = " ".join([l.strip() for l in lines[max(0, j-1):min(len(lines), j+2)]])
                    # Look for explicit percentages representing funding ratios
                    if re.search(r'1\d{2}(?:,\d{1,2})?\s*%', context) or 'minimaal' in context.lower():
                        print(f"Pg {i+1}: {line.strip()}")
                        found = True
            
            if found and i > 10: break # if we found sufficient hits in the first 10 pages, move to next PDF
            
    except Exception as e:
        print(f"Failed: {e}")
