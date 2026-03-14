from pypdf import PdfReader

pdf_path = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen/data/reports/152_BPF Foodservice.pdf"

try:
    pdf = PdfReader(pdf_path)
    print("PDF loaded successfully. Total pages:", len(pdf.pages))
    
    # Extract page 7 (index 6), page 8 (index 7), and page 9 (index 8) to be safe
    for i in range(6, 9):
        print(f"\n================ PAGE {i+1} ================")
        text = pdf.pages[i].extract_text()
        if text:
            print(text[:2000])
        else:
            print("No text found on this page.")
                
except Exception as e:
    print(f"Failed to fetch or parse PDF: {e}")
