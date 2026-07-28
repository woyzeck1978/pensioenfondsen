import PyPDF2

pdf_path = "data/reports/78_Atos.pdf"

print("Searching for rendement...")
try:
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        for p in range(len(reader.pages)):
            text = reader.pages[p].extract_text()
            if text and ("rendement" in text.lower() or "beleggen" in text.lower()):
                lines = text.split('\\n')
                for i, line in enumerate(lines):
                    if "rendement" in line.lower() and "%" in line:
                         print(f"\\n--- PAGE {p+1} Match ---")
                         start = max(0, i-2)
                         end = min(len(lines), i+3)
                         print("\\n".join(lines[start:end]))
                
except Exception as e:
    print(f"Error reading PDF: {e}")
