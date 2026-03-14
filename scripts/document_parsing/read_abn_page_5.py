import fitz

pdf_path = "data/reports/71_ABN_Amro_Jaarverslag_2024.pdf"
doc = fitz.open(pdf_path)

# Extract pages 3 to 7 (index 2 to 6)
for i in range(2, 7):
    print(f"--- PAGE {i+1} (0-indexed {i}) ---")
    print(doc.load_page(i).get_text())
    print("----------------------------\n")
