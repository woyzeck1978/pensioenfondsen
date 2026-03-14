import requests
from pypdf import PdfReader
from io import BytesIO
import urllib3
import re

urllib3.disable_warnings()

url = "https://hnpf.nl/site/wp-content/uploads/HNPF-Jaarverslag-2024-DEF-website-en-KVK.pdf"
headers = {'User-Agent': 'Mozilla/5.0'}

print("Fetching HNPF Jaarverslag 2024 PDF...")
try:
    response = requests.get(url, headers=headers, verify=False, timeout=30)
    response.raise_for_status()
    pdf = PdfReader(BytesIO(response.content))
    
    # We want to extract 'Beleidsdekkingsgraad' and 'Belegd vermogen' or 'Vermogen' per Kring (Arcadis, Sweco, Cargill, OWASE)
    print("\n--- Hunting for Kring Financials ---")
    
    kringen = ['Arcadis', 'Sweco', 'Cargill', 'OWASE']
    
    for i, page in enumerate(pdf.pages):
        # We scan the first 80 pages where management summaries are
        if i > 80: break
        text = page.extract_text()
        if not text: continue
        
        lines = text.split('\n')
        for j, line in enumerate(lines):
            lower_line = line.lower()
            
            # Hunting for AUM (vermogen / mln / mld)
            if any(k.lower() in lower_line for k in kringen) and ('vermogen' in lower_line or 'mln' in lower_line or 'dekkingsgraad' in lower_line):
                # Grab surrounding context
                context = " | ".join([l.strip() for l in lines[max(0, j-2):min(len(lines), j+2)]])
                print(f"Pg {i+1}: {context}")
                
except Exception as e:
    print(f"Failed to fetch or parse PDF: {e}")
