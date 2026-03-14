import os
from pypdf import PdfReader

pdf_path = "data/bpfbouw-jaarverslag-2024.pdf"
reader = PdfReader(pdf_path)

keywords = ['dekkingsgraad', 'beleidsdekkingsgraad', 'vermogen', 'beheerkosten', 'uitvoeringskosten', 'deelnemers', 'toeslag', 'indexatie', 'rendement', 'beleggingsmix', 'aandelen']

for i, page in enumerate(reader.pages):
    text = page.extract_text()
    if text:
        lines = text.split('\n')
        for j, line in enumerate(lines):
            lower_line = line.lower()
            if any(kw in lower_line for kw in keywords):
                # Print just the line if it has a number
                if any(char.isdigit() for char in line):
                    print(f"[Page {i+1}] {line.strip()}")
