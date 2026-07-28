import PyPDF2

pdf_path = "data/reports/152_Foodservice_2023.pdf"

try:
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        # Search the whole document for "kerncijfers", "kengetallen", "belegd vermogen"
        for p in range(len(reader.pages)):
            text = reader.pages[p].extract_text()
            text_lower = text.lower() if text else ""
            if "kernboodschap" in text_lower or "kerncijfer" in text_lower or "kengetallen" in text_lower or "meerjaren" in text_lower:
                lines = text.split('\\n')
                for i, line in enumerate(lines):
                    if "2023" in line and ("2022" in line or "2021" in line):
                        print(f"\\n--- PAGE {p+1} Match ---")
                        start = max(0, i-5)
                        end = min(len(lines), i+15)
                        print("\\n".join(lines[start:end]))
                        break
except Exception as e:
    print(f"Error reading PDF: {e}")
