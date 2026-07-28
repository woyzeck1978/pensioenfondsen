import PyPDF2

pdf_path = "data/reports/70_Abbott  Abbott Nederland.pdf"

print(f"Extracting Abbott Key figures from {pdf_path}")
try:
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        
        # PyPDF2 is 0-indexed. Pages 8, 11, and 12 map to index 7, 10, and 11.
        target_pages = [7, 10, 11]
        
        for p in target_pages:
            try:
                text = reader.pages[p].extract_text()
                if text:
                    print(f"\\n--- PAGE {p+1} ---\\n")
                    print(text)
            except Exception as e:
                print(f"Error reading page {p+1}: {e}")
                
except Exception as e:
    print(f"Error opening PDF: {e}")
