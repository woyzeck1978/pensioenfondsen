import fitz

pdf_path = "data/reports/71_ABN_Amro_Jaarverslag_2024.pdf"
doc = fitz.open(pdf_path)

# Page 81 (index 80) is where total investments are listed
page = doc.load_page(80) 
text = page.get_text()

lines = text.split('\n')
for i, line in enumerate(lines):
    if "Totaal beleggingen" in line or "Totaal Beleggingen" in line:
        start = max(0, i-2)
        end = min(len(lines), i+3)
        print("MATCH TOTAL:")
        for k in range(start, end):
            print(lines[k])
        print("---")
        
    if "Aandelen" in line:
        start = max(0, i-2)
        end = min(len(lines), i+3)
        print("MATCH EQUITY:")
        for k in range(start, end):
            print(lines[k])
        print("---")
