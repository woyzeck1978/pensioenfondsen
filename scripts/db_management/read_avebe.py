import PyPDF2
import re

pdf_path = "data/reports/79_Avebe.pdf"

try:
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        
        for p in range(len(reader.pages)):
            text = reader.pages[p].extract_text()
            text_lower = text.lower() if text else ""
            
            # participants
            if "deelnemers" in text_lower and "aantal" in text_lower:
                lines = text.split('\\n')
                for i, line in enumerate(lines):
                    if "actiev" in line.lower() or "slaper" in line.lower() or "gewezen" in line.lower() or "gepensioneerd" in line.lower():
                        if re.search(r'\\d+', line): # has numbers
                             print(f"\\n--- PAGE {p+1} (Participants) ---")
                             start = max(0, i-2)
                             end = min(len(lines), i+3)
                             print("\\n".join(lines[start:end]))
                             
            # rendement
            if "rendement" in text_lower and ("%" in text_lower or "procent" in text_lower):
                lines = text.split('\\n')
                for i, line in enumerate(lines):
                    if "totaal" in line.lower() and "rendement" in line.lower():
                        print(f"\\n--- PAGE {p+1} (Rendement) ---")
                        start = max(0, i-2)
                        end = min(len(lines), i+3)
                        print("\\n".join(lines[start:end]))
                        
except Exception as e:
    print(f"Error reading PDF: {e}")
