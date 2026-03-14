from pypdf import PdfReader

pdf_path = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen/data/reports/106_Hoogovens.pdf"

try:
    pdf = PdfReader(pdf_path)
    print("\n--- Index 14 ---")
    print(pdf.pages[14].extract_text()[:1000])
    print("\n--- Index 15 ---")
    print(pdf.pages[15].extract_text()[:1000])
    print("\n--- Index 16 ---")
    print(pdf.pages[16].extract_text()[:1000])
except Exception as e:
    print(f"Failed: {e}")
