import PyPDF2

pdf_path = "data/reports/9_ABP Government and Education.pdf"
try:
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        # Note: PyPDF2 is 0-indexed. Page 178 in the document is likely index 177. Let's extract 177 to 182.
        for p in range(177, 182):
            try:
                page = reader.pages[p]
                text = page.extract_text()
                print(f"--- PAGE {p+1} ---")
                print(text)
                print("-" * 20)
            except Exception as e:
                print(f"Error on page {p+1}: {e}")
except Exception as e:
    print(f"Failed to open PDF: {e}")
