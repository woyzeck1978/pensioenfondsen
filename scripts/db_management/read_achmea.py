import PyPDF2

pdf_path = "data/reports/72_Achmea.pdf"

print(f"Extracting Achmea Key figures from {pdf_path}")
try:
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        # Note: PyPDF2 is 0-indexed, so page 8 is index 7
        for p in range(0, 2):
            try:
                page = reader.pages[p]
                text = page.extract_text()
                print(f"\\n--- PAGE {p+1} ---\\n")
                print(text)
            except Exception as e:
                pass
except Exception as e:
    print(f"Error: {e}")
