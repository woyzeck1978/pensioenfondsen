import fitz

pdf_path = "data/reports/108_ING  ING CDC.pdf"
doc = fitz.open(pdf_path)

# Extract pages 2 to 5 (index 1 to 4) to ensure we cover "page 3 & 4" which might be physical or logical
for i in range(1, 6):
    print(f"--- PAGE {i+1} (0-indexed {i}) ---")
    try:
        print(doc.load_page(i).get_text())
    except:
        pass
    print("----------------------------\n")
