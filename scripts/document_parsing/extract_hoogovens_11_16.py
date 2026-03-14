from pypdf import PdfReader

pdf_path = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen/data/reports/106_Hoogovens.pdf"

try:
    pdf = PdfReader(pdf_path)
    
    # Let's just output indices 3, 4, 5, 6, 7 to a text file
    with open("hoogovens_11_16.txt", "w") as f:
        f.write("==== PAGE 11 ==== (guessing index 5 or 6)\n")
        f.write(pdf.pages[5].extract_text())
        f.write("\n==== PAGE 11 fallback ==== (index 4)\n")
        f.write(pdf.pages[4].extract_text())
        f.write("\n==== PAGE 11 fallback ==== (index 3)\n")
        f.write(pdf.pages[3].extract_text())
        
except Exception as e:
    print(f"Failed: {e}")
