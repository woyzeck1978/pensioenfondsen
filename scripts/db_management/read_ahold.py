import PyPDF2

pdf_path = "data/reports/73_Ahold_Delhaize.pdf"

print("Extracting Ahold Delhaize 2024 Annual Report...")

try:
    with open(pdf_path, 'rb') as pdf_file:
        reader = PyPDF2.PdfReader(pdf_file)
        
        num_pages = len(reader.pages)
        print(f"Total pages: {num_pages}")
        
        print("\\n--- EXTRACTING KEY FIGURES (First 15 Pages) ---")
        for p in range(0, min(15, num_pages)):
            try:
                text = reader.pages[p].extract_text()
                if text:
                    print(f"\\n--- PAGE {p+1} ---\\n")
                    print(text)
            except Exception as e:
                pass
                
        print("\\n--- EXTRACTING SFDR (Last 30 Pages) ---")
        start_sfdr = max(0, num_pages - 30)
        for p in range(start_sfdr, num_pages):
            try:
                text = reader.pages[p].extract_text()
                if text:
                    for line in text.split('\\n'):
                        line_lower = line.lower()
                        if "artikel 8" in line_lower or "artikel 9" in line_lower or ("taxonomie" in line_lower and "%" in line):
                            print(f"[PAGE {p+1}] Match: {line}")
            except Exception as e:
                pass
            
except Exception as e:
    print(f"Error: {e}")
