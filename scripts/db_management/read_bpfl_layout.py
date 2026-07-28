import pdfplumber

pdf_path = "data/processed/bpfl_2024.pdf"
pages_to_extract = [32, 33, 34, 106, 107, 108]
output_path = "/tmp/bpfl_layout.txt"

try:
    with pdfplumber.open(pdf_path) as pdf:
        with open(output_path, "w") as f:
            for p in pages_to_extract:
                if p < len(pdf.pages):
                    f.write(f"--- PAGE {p+1} ---\n\n")
                    f.write(pdf.pages[p].extract_text(layout=True) or "")
                    f.write("\n\n")
    print(f"Successfully extracted layout text to {output_path}")
except Exception as e:
    print(f"Error: {e}")
