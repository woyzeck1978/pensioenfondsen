import fitz

pdf_path = "data/reports/108_ING  ING CDC.pdf"
doc = fitz.open(pdf_path)

# Extract page 25 (0-indexed 24) and surrounding pages just in case
for i in range(23, 27):
    print(f"--- PAGE {i+1} (0-indexed {i}) ---")
    try:
        print(doc.load_page(i).get_text())
    except Exception as e:
        print(f"Error reading page {i+1}: {e}")
    print("----------------------------\n")
