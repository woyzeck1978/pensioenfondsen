import os
from pypdf import PdfReader

pdf_path = "../data/reports/152_BPF Foodservice.pdf"

if os.path.exists(pdf_path):
    reader = PdfReader(pdf_path)
    print(f"Reading {pdf_path}... Pages: {len(reader.pages)}")
    
    for i, page in enumerate(reader.pages):
        if i > 40: break
        text = page.extract_text()
        if text:
            # Look for lines with vermogen, milj, mld
            lines = text.split('\n')
            for j, line in enumerate(lines):
                if 'vermogen' in line.lower() or 'miljoen' in line.lower() or 'miljard' in line.lower() or 'mld' in line.lower():
                    # Print context
                    start = max(0, j-1)
                    end = min(len(lines), j+2)
                    print(f"--- Page {i+1} Context ---")
                    for k in range(start, end):
                        print(f"[{k}] {lines[k].strip()}")
else:
    print("File not found.")
