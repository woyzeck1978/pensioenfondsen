import fitz
doc = fitz.open("data/reports/71_ABN_Amro_Jaarverslag_2024.pdf")
for i in range(41, 47):
    print(f"--- PAGE {i} (0-indexed) ---")
    print(doc.load_page(i).get_text())
