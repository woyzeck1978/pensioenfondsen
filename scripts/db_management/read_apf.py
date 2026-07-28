import PyPDF2

pdfs = [
    "data/reports/64_APF Het Nederlandse Pensioenfonds.pdf",
    "data/reports/75_APF AkzoNobel Nouryon Nobian Salt.pdf",
    "data/reports/148_APF.pdf"
]

for pdf in pdfs:
    print(f"\\n{'='*50}\\nFILE: {pdf}\\n{'='*50}")
    try:
        with open(pdf, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            print(f"Total pages: {len(reader.pages)}")
            # Pages 6-7 (indexes 5, 6, 7 to be safe)
            for p in range(5, min(8, len(reader.pages))):
                try:
                    page = reader.pages[p]
                    text = page.extract_text()
                    if text and ("kerncijfers" in text.lower() or "dekkingsgraad" in text.lower() or "rendement" in text.lower()):
                        print(f"--- PAGE {p+1} MATCH ---")
                        print(text[:1000] + "...\\n")
                except Exception as e:
                    pass
    except Exception as e:
        print(f"Error: {e}")
