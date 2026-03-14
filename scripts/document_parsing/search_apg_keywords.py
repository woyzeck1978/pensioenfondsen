from pypdf import PdfReader
import re

pdf_path = "data/reports/76_APG.pdf"

with open(pdf_path, 'rb') as file:
    reader = PdfReader(file)
    print(f"Total pages: {len(reader.pages)}")
    
    for i, page in enumerate(reader.pages):
        text = page.extract_text()
        if text:
            # Look for lines with percentages and "aandelen"
            lines = text.split('\n')
            for j, line in enumerate(lines):
                if re.search(r'aandelen|rendementsportefeuille|allocatie|beleggingsmix', line, re.IGNORECASE):
                    # check if the line has a number or if adjacent lines do
                    if re.search(r'\d+', line):
                        start = max(0, j-1)
                        end = min(len(lines), j+2)
                        print(f"--- PAGE {i+1} : Context (L{j}) ---")
                        for k in range(start, end):
                            print(f"{lines[k]}")
                        print("---")
