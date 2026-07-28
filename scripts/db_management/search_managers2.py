import PyPDF2
import re

pdf_path = "data/reports/73_Ahold_Delhaize.pdf"

print("Searching for asset managers in the Ahold Delhaize PDF...")

try:
    with open(pdf_path, 'rb') as pdf_file:
        reader = PyPDF2.PdfReader(pdf_file)
        
        num_pages = len(reader.pages)
        
        keywords = ["blackrock", "state street", "northern trust", "kempen", "robeco", "achmea investment", "apg", "pggm", "mn ", "nn ip", "aegon", "schroders", "amundi", "fiduciary", "fiduciair"]
        
        found = False
        for p in range(0, num_pages):
            try:
                text = reader.pages[p].extract_text()
                if text:
                    text_lower = text.lower()
                    for kw in keywords:
                        if kw in text_lower:
                            # Print a snippet around the keyword
                            # To be safe, let's just print the sentence/lines
                            lines = text.split('\\n')
                            for i, line in enumerate(lines):
                                if kw in line.lower():
                                    start = max(0, i-2)
                                    end = min(len(lines), i+3)
                                    print(f"\\n--- PAGE {p+1} (Match: {kw}) ---")
                                    print("\\n".join(lines[start:end]))
                                    found = True
            except Exception as e:
                pass
except Exception as e:
    print(f"Error: {e}")
