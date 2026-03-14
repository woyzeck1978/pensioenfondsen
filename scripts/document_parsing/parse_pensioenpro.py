import os
from pypdf import PdfReader

reports = [
    "../data/PensioenPro/Invaarplanning-van-april-tm-oktober-2026.pdf",
    "../data/PensioenPro/Overzicht-pensioentransitie-per-pensioenfonds-11-2-2026.pdf"
]

for pdf_path in reports:
    print(f"\n=============================================")
    print(f"Reading {pdf_path}...")
    if not os.path.exists(pdf_path):
        print("File not found.")
        continue
        
    reader = PdfReader(pdf_path)
    print(f"Pages: {len(reader.pages)}")
    
    # Dump just the first two pages to understand the table schema
    for i in range(min(2, len(reader.pages))):
        print(f"\n--- Page {i+1} ---")
        text = reader.pages[i].extract_text()
        # Print first 25 lines
        for j, line in enumerate(text.split('\n')):
            if j < 25:
                print(line.strip())
