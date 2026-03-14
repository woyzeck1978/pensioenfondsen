from pypdf import PdfReader

pdf_path = "data/reports/76_APG.pdf"

try:
    with open(pdf_path, 'rb') as file:
        reader = PdfReader(file)
        print(f"Total pages: {len(reader.pages)}")
        
        # Try indices around 53 (51, 52, 53, 54) in case of 0-indexing / cover page offsets
        for page_num in [51, 52, 53, 54]:
            try:
                page = reader.pages[page_num]
                text = page.extract_text()
                print(f"--- PAGE {page_num + 1} (INDEX {page_num}) ---")
                print(text)
                print("------------------\n")
            except IndexError:
                pass
except Exception as e:
    print(f"Error: {e}")
