import fitz

pdf_path = "data/reports/106_Hoogovens.pdf"
doc = fitz.open(pdf_path)

for i in range(10, 14):
    print(f"--- PAGE {i+1} (0-indexed {i}) ---")
    try:
        print(doc.load_page(i).get_text())
    except Exception as e:
        print(f"Error reading page {i+1}: {e}")
    print("----------------------------\n")
