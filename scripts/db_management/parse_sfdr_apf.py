import PyPDF2
import re

pdf_path = "data/reports/75_APF AkzoNobel Nouryon Nobian Salt.pdf"

print("Extracting APF SFDR data from page 148 onwards...")
try:
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        # Note: PyPDF2 is 0-indexed, so page 148 is index 147. Let's read 147 to 155.
        
        for p in range(151, 156):
            try:
                page = reader.pages[p]
                text = page.extract_text()
                print(f"\\n--- PAGE {p+1} ---\\n")
                print(text)
            except Exception as e:
                pass
except Exception as e:
    print(f"Error: {e}")
