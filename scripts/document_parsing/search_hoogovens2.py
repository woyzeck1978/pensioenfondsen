from pypdf import PdfReader

pdf_path = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen/data/reports/106_Hoogovens.pdf"

try:
    pdf = PdfReader(pdf_path)
    # Search indices 10 through 25
    for i in range(10, 25):
        text = pdf.pages[i].extract_text()
        if not text: continue
        
        # We are looking for the word Aandelen and percentages
        if 'aandelen' in text.lower():
            print(f"\n================ FULL TEXT OF INDEX {i} ================")
            print(text)
            
except Exception as e:
    print(f"Failed: {e}")
