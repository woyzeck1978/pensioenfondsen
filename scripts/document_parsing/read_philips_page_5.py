import fitz

pdf_path = "data/reports/119_Philips.pdf"
doc = fitz.open(pdf_path)

# Extract pages 4, 5, 6 (index 3, 4, 5)
for i in range(3, 6):
    print(f"--- PAGE {i+1} (0-indexed {i}) ---")
    try:
        print(doc.load_page(i).get_text())
    except Exception as e:
        print(f"Error reading page {i+1}: {e}")
    print("----------------------------\n")
