import fitz
import re

pdf_path = "data/reports/119_Philips.pdf"
doc = fitz.open(pdf_path)

print("Searching for 'Kerncijfers'...")
for i in range(len(doc)):
    page = doc.load_page(i)
    text = page.get_text()
    if re.search(r'kerncijfer', text, re.IGNORECASE):
        print(f"Found 'kerncijfer' on page {i+1}")
        
print("\nSearching for 'Portefeuilleverdeling'...")
for i in range(len(doc)):
    page = doc.load_page(i)
    text = page.get_text()
    if re.search(r'portefeuilleverdeling|beleggingsmix|asset allocatie|asset mix', text, re.IGNORECASE):
        print(f"Found match on page {i+1}")
