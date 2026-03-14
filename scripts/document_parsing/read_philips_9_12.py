import fitz

pdf_path = "data/reports/119_Philips.pdf"
doc = fitz.open(pdf_path)

print("--- PAGE 9 (Kerncijfers) ---")
print(doc.load_page(8).get_text())

print("\n--- PAGE 12 (Portefeuilleverdeling) ---")
print(doc.load_page(11).get_text())
