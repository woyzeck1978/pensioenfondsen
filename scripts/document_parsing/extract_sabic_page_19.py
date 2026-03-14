from pypdf import PdfReader

pdf_path = "data/reports/126_SABIC.pdf"

with open(pdf_path, 'rb') as file:
    reader = PdfReader(file)
    print(f"Total pages: {len(reader.pages)}")
    
    # Try indices around 19 (17, 18, 19, 20) in case of 0-indexing / cover page offsets
    for page_num in range(15, 26):
        try:
            page = reader.pages[page_num]
            text = page.extract_text()
            print(f"--- PAGE {page_num + 1} (INDEX {page_num}) ---")
            print(text)
            print("------------------\n")
        except IndexError:
            pass
