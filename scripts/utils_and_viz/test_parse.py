import fitz
import re

pdf_path = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen/data/reports/83_Capgemini  Capgemini Nederland.pdf"
doc = fitz.open(pdf_path)
page = doc[6] # Page 7
text = page.get_text()

lines = [line.strip() for line in text.split('\n') if line.strip()]

years_found = []
for line in lines:
    matches = re.findall(r'^(202[0-4])$', line)
    if not matches:
        # Sometimes they are on the same line
        matches = re.findall(r'(202[0-4])', line)
    if len(matches) >= 3:
        years_found = matches
        break
        
print("Detected Year Columns:", years_found)

page_text_flattened = " ".join(lines).lower()

metrics_patterns = {
    'beleggingsrendement_pct': r'(?:belegging|rendement)[^\d]+((?:-?\d+[,.]\d+\s*%?\s*){3,5})',
    'beleidsdekkingsgraad_pct': r'beleids?dekkingsgraad[^\d]+((?:-?\d+[,.]\d+\s*%?\s*){3,5})',
    'economische_dekkingsgraad_pct': r'(?:actuele|economische) dekkingsgraad[^\d]+((?:-?\d+[,.]\d+\s*%?\s*){3,5})',
    'indexatieverlening_pct': r'(?:indexatie|toeslag|verhoging)[^\d]+((?:-?\d+[,.]\d+\s*%?\s*){3,5})',
}

for field, pattern in metrics_patterns.items():
    match = re.search(pattern, page_text_flattened)
    if match:
        numbers_str = match.group(1)
        numbers = re.findall(r'-?\d+[,.]\d+', numbers_str)
        print(f"{field}:", numbers)
    else:
        print(f"Could not extract {field}")

