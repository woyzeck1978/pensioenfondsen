import fitz  # PyMuPDF

pdf_path = "data/reports/71_ABN_Amro_Jaarverslag_2024.pdf"
try:
    doc = fitz.open(pdf_path)
    print(f"Total pages: {len(doc)}")
    
    # Try pages around 44 (42, 43, 44, 45, 46) for exact match
    for page_num in range(41, 46):
        try:
            page = doc.load_page(page_num)
            text = page.get_text()
            print(f"--- PAGE {page_num + 1} (INDEX {page_num}) ---")
            print(text)
            print("------------------\n")
        except Exception as e:
            print(f"Error reading page {page_num}: {e}")
except Exception as e:
    print(f"Error opening PDF: {e}")
