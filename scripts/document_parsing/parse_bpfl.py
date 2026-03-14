import os
import re
from pypdf import PdfReader

pdf_path = "/Users/webkowuite/.gemini/antigravity/brain/2aabfd7b-9b79-4b44-9eb9-149ceaad91df/.tempmediaStorage/0f2a106034a34900.pdf"

try:
    pdf = PdfReader(pdf_path)
    
    print("\n--- Extracting BPFL Transitieplan Metrics ---")
    
    found_data = []
    for i, page in enumerate(pdf.pages):
        if i > 40: break
        text = page.extract_text()
        if not text: continue
        
        lines = text.split('\n')
        for j, line in enumerate(lines):
            lower_line = line.lower()
            if ('invaren' in lower_line or 'dekkingsgraad' in lower_line or 'indexatie' in lower_line) and ('%' in line):
                context = " ".join([l.strip() for l in lines[max(0, j-1):min(len(lines), j+2)]])
                if re.search(r'\d{1,3}(?:,\d{1,2})?\s*%', context):
                    found_data.append(f"Pg {i+1}: {line.strip()}")
    
    for param in found_data[:20]:
        print(param)
                
except Exception as e:
    print(f"Failed to fetch or parse PDF: {e}")
