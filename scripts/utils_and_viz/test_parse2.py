import fitz
import re
from collections import defaultdict

pdf_path = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen/data/reports/83_Capgemini  Capgemini Nederland.pdf"
doc = fitz.open(pdf_path)
page = doc[6]

blocks = page.get_text("dict")["blocks"]

# Group text spans by their vertical block / Y-coordinate to reconstruct rows
rows = defaultdict(list)
for b in blocks:
    if "lines" in b:
        for l in b["lines"]:
            for s in l["spans"]:
                # Round Y0 to nearest 5 pixels to group same-line items
                y_coord = round(s["bbox"][1] / 5.0) * 5
                rows[y_coord].append((s["bbox"][0], s["text"].strip()))

# Sort rows by Y-coordinate
sorted_y = sorted(rows.keys())

# Find the row with years
year_row = None
year_positions = [] # list of (x_coord, year_str)
for y in sorted_y:
    items = sorted(rows[y], key=lambda x: x[0])
    years = []
    for x_ptr, txt in items:
        if re.match(r'^202[0-4]$', txt):
            years.append((x_ptr, txt))
    if len(years) >= 3:
        year_row = y
        year_positions = years
        break

print("Found year headers:")
for x, yr in year_positions:
    print(f"  {yr} at X:{x}")

metrics_keywords = {
    'beleggingsrendement_pct': ['beleggingsrendement', 'rendement'],
    'economische_dekkingsgraad_pct': ['actuele dekkingsgraad', 'economische dekkingsgraad'],
    'beleidsdekkingsgraad_pct': ['beleidsdekkingsgraad'],
    'indexatieverlening_pct': ['indexatie', 'toeslag', 'verhoging']
}

for y in sorted_y:
    if year_row is None or y <= year_row: continue # Skip headers and above
    
    items = sorted(rows[y], key=lambda x: x[0])
    row_text = " ".join([txt for _, txt in items]).lower()
    
    for metric, kws in metrics_keywords.items():
        if any(kw in row_text for kw in kws):
            print(f"\nRow matched {metric}: {row_text}")
            # Map numbers to the closest year column
            numbers = []
            for x, txt in items:
                # Find if it's a number
                if re.match(r'^-?\d+[,.]?\d*$', txt.replace('%','').strip()):
                    numbers.append((x, txt))
            
            for nx, ntxt in numbers:
                # Find closest year
                closest_year = min(year_positions, key=lambda yp: abs(yp[0] - nx))[1]
                print(f"  Mapped {ntxt} to {closest_year}")
