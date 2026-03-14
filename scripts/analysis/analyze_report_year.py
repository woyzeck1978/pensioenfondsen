import os
import sqlite3
import re
import fitz # PyMuPDF
from concurrent.futures import ProcessPoolExecutor, as_completed

# Database and directory setup
DB_PATH = "../../data/processed/pension_funds.db"
REPORTS_DIR = "../data/reports"

def extract_text_from_pdf(pdf_path, max_pages=10):
    text = ""
    try:
        doc = fitz.open(pdf_path)
        for i in range(min(max_pages, len(doc))):
            text += doc[i].get_text() + "\n"
    except Exception as e:
        pass
    return text.lower()

def analyze_report_year(filename):
    pdf_path = os.path.join(REPORTS_DIR, filename)
    fund_name_from_file = filename.replace('.pdf', '')
    try:
        fund_id = int(fund_name_from_file.split('_')[0])
    except ValueError:
        return None

    text = extract_text_from_pdf(pdf_path, max_pages=15)
    if not text:
        return None

    year = None
    
    # 1. Best case: "jaarverslag 2024" or "annual report 2023" very close together
    match = re.search(r'(jaarverslag|annual report).{0,15}(202[0-4])', text)
    if match:
        year = int(match.group(2))
    else:
        # 2. Reverse: "2024 ... jaarverslag"
        match = re.search(r'(202[0-4]).{0,15}(jaarverslag|annual report)', text)
        if match:
             year = int(match.group(1))

    # 3. Fallback: Most common year mentioned in the first 15 pages (2020 to 2024)
    if not year:
        years_found = re.findall(r'\b(202[0-4])\b', text)
        if years_found:
            # Count frequencies
            counts = {}
            for y in years_found:
                counts[y] = counts.get(y, 0) + 1
            # Return most frequent year
            best_y = max(counts, key=counts.get)
            year = int(best_y)

    return fund_id, year

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    pdf_files = [f for f in os.listdir(REPORTS_DIR) if f.endswith('.pdf')]
    print(f"Found {len(pdf_files)} reports to scan for the reporting year.")

    results = []
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = {executor.submit(analyze_report_year, f): f for f in pdf_files}
        
        for i, future in enumerate(as_completed(futures), 1):
            filename = futures[future]
            try:
                res = future.result()
                if res is not None:
                    fund_id, year = res
                    if year:
                        results.append((year, fund_id))
                        print(f"[{i}/{len(pdf_files)}] {filename} -> Year: {year}")
            except Exception as e:
                print(f"Error processing {filename}: {e}")

    if results:
        c.executemany("UPDATE funds SET annual_report_year = ? WHERE id = ?", results)
        conn.commit()
    conn.close()
    
    print("Done! Extracted reporting years for reports.")

if __name__ == "__main__":
    main()
