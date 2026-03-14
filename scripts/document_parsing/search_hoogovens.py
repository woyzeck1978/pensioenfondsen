import fitz
import re

pdf_path = "data/reports/106_Hoogovens.pdf"
doc = fitz.open(pdf_path)

print("Searching for 'aandelen' or 'portefeuilleverdeling' or 'beleggingsmix'...")
for i in range(len(doc)):
    page = doc.load_page(i)
    text = page.get_text()
    if re.search(r'aandelen|portefeuilleverdeling|beleggingsmix|asset allocatie|asset mix', text, re.IGNORECASE):
        print(f"Found match on page {i+1}")
