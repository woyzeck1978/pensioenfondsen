import fitz  # PyMuPDF
import re

pdf_path = "data/reports/71_ABN_Amro_Jaarverslag_2024.pdf"
try:
    doc = fitz.open(pdf_path)
    print(f"Total pages: {len(doc)}")
    
    for i in range(len(doc)):
        page = doc.load_page(i)
        text = page.get_text()
        
        if text:
            lines = text.split('\n')
            for j, line in enumerate(lines):
                # Search for equity/allocation keywords
                if re.search(r'aandelen|zakelijk|beleggingsmix|allocatie', line, re.IGNORECASE):
                    # check if the line has a number or if adjacent lines do
                    if re.search(r'\d+', line) or (j < len(lines)-1 and re.search(r'\d+', lines[j+1])):
                        start = max(0, j-2)
                        end = min(len(lines), j+3)
                        print(f"--- PAGE {i+1} : Context (L{j}) ---")
                        for k in range(start, end):
                            print(f"{lines[k]}")
                        print("---")
except Exception as e:
    print(f"Error: {e}")
