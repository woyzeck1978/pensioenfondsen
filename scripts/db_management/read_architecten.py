import PyPDF2

pdf_path = "data/reports/10_Architectenbureaus.pdf"

print("Extracting Kerncijfers (page 6 and 7)...")
try:
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        
        # pages 6 and 7 are index 5 and 6
        for p in [5, 6]:
            print(f"\\n--- PAGE {p+1} ---\\n")
            print(reader.pages[p].extract_text())
                
except Exception as e:
    print(f"Error reading PDF: {e}")
