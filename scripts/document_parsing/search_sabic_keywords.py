from pypdf import PdfReader
import re

pdf_path = "data/reports/126_SABIC.pdf"

with open(pdf_path, 'rb') as file:
    reader = PdfReader(file)
    print(f"Total pages: {len(reader.pages)}")
    
    for i, page in enumerate(reader.pages):
        text = page.extract_text()
        if text:
            if re.search(r'aandele', text, re.IGNORECASE) or re.search(r'zakelijk', text, re.IGNORECASE) or re.search(r'rendementsportefeuille', text, re.IGNORECASE):
                print(f"--- INDEX {i} ---")
                
                # Print lines containing the keywords, plus a bit of context
                lines = text.split('\n')
                for j, line in enumerate(lines):
                    if re.search(r'aandele|zakelijk|rendementsportefeuille', line, re.IGNORECASE):
                        start = max(0, j-2)
                        end = min(len(lines), j+3)
                        print(f"Context (L{j}):")
                        for k in range(start, end):
                            print(f"{lines[k]}")
                        print("---")
