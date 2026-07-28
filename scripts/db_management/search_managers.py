import PyPDF2
import re

pdf_path = "data/reports/73_Ahold_Delhaize.pdf"

print("Searching Ahold Delhaize 2024 Annual Report for Equity Portfolio Managers...")

try:
    with open(pdf_path, 'rb') as pdf_file:
        reader = PyPDF2.PdfReader(pdf_file)
        
        num_pages = len(reader.pages)
        
        keywords = ["aandelen", "vermogensbeheerder", "beheerder", "managers", "mandaten", "blackrock", "state street", "robeco", "kempen", "achmea", "apg", "pggm", "mn", "f&c", "allianz", "amundi", "schroders", "aegon", "nnip"]
        
        for p in range(0, num_pages):
            try:
                text = reader.pages[p].extract_text()
                if text:
                    text_lower = text.lower()
                    if "aandelen" in text_lower and any(kw in text_lower for kw in ["beheerder", "mandaat", "pool", "fonds"]):
                        # Print surrounding text for context
                        lines = text.split('\\n')
                        for i, line in enumerate(lines):
                            if "aandelen" in line.lower():
                                start = max(0, i - 5)
                                end = min(len(lines), i + 6)
                                print(f"\\n--- PAGE {p+1} ---")
                                print("\\n".join(lines[start:end]))
                                break
            except Exception as e:
                pass
except Exception as e:
    print(f"Error: {e}")
