import fitz
import requests
import re
import urllib3
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Download Rijn & Binnenvaart
rb_url = 'https://www.rijnenbinnenvaartpensioen.nl/media/16042/jaarverslag-2023-in-beeld.pdf'
try:
    if not os.path.exists('data/processed/rb-jaarverslag-2023.pdf'):
        pdf_resp = requests.get(rb_url, verify=False, headers={'User-Agent': 'Mozilla/5.0'})
        with open('data/processed/rb-jaarverslag-2023.pdf', 'wb') as f:
            f.write(pdf_resp.content)

    print('\n=== RIJN & BINNENVAART AUM ===')
    doc_rb = fitz.open('data/processed/rb-jaarverslag-2023.pdf')
    for i in range(min(20, len(doc_rb))):
        text = doc_rb[i].get_text()
        if 'vermogen' in text.lower() or 'totaal' in text.lower() or 'belegging' in text.lower():
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            for line in lines:
                if 'vermogen' in line.lower() or 'belegging' in line.lower() or 'miljoen' in line.lower() or 'miljard' in line.lower():
                    if re.search(r'\d', line):
                        print(f'R&B Page {i+1}: {line}')
except Exception as e:
    print('R&B Error:', e)

# Deep dive BPL Pensioen
print('\n=== BPL PENSIOEN DEEP DIVE ===')
try:
    doc_bpl = fitz.open('data/processed/bpl-pensioen-jaarverslag-2024.pdf')
    for i in range(15, 60):
        text = doc_bpl[i].get_text()
        if 'vermogen' in text.lower() or 'balans' in text.lower() or 'totaal' in text.lower():
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            for line in lines:
                if 'vermogen' in line.lower() or 'belegging' in line.lower() or 'totaal' in line.lower():
                    if 'milj' in line.lower() or re.search(r'€.*\d', line):
                        print(f'BPL Page {i+1}: {line}')

except Exception as e:
    print('BPL Deep Dive Error:', e)

