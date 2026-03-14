from pypdf import PdfReader

pdf_path = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen/data/reports/152_BPF Foodservice.pdf"

try:
    pdf = PdfReader(pdf_path)
    # The user says "page 8". PdfReader is 0-indexed, so page 8 is index 7. 
    # I will also grab index 8 just in case they meant the structural 8th page.
    print(f"\n================ BPF Foodservice (Page 8) ================")
    text_7 = pdf.pages[7].extract_text()
    if text_7:
        print("--- Index 7 ---")
        print(text_7[:1500]) # First 1500 chars 
        
    text_8 = pdf.pages[8].extract_text()
    if text_8:
        print("\n--- Index 8 ---")
        print(text_8[:1500])
                
except Exception as e:
    print(f"Failed to fetch or parse PDF: {e}")
