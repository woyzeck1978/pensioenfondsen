from pypdf import PdfReader

pdf_path = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen/data/reports/106_Hoogovens.pdf"

try:
    pdf = PdfReader(pdf_path)
    print("====== INDICES 3 to 9 ======")
    for i in range(3, 10):
        print(f"\n>>>> INDEX {i} <<<<")
        print(pdf.pages[i].extract_text())
except Exception as e:
    print(f"Failed: {e}")
