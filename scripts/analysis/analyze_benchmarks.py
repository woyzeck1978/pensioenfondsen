import os
import sqlite3
import re
import fitz # PyMuPDF
from concurrent.futures import ProcessPoolExecutor, as_completed

# Database and directory setup
DB_PATH = "../../data/processed/pension_funds.db"
REPORTS_DIR = "../data/reports"

def extract_text_from_pdf(pdf_path, max_pages=150):
    text = ""
    try:
        doc = fitz.open(pdf_path)
        for page_num in range(min(max_pages, len(doc))):
            text += doc.load_page(page_num).get_text() + " "
    except Exception as e:
        pass
    return text.lower().replace('\\n', ' ')

def analyze_benchmarks(filename):
    pdf_path = os.path.join(REPORTS_DIR, filename)
    fund_name_from_file = filename.replace('.pdf', '')
    try:
        fund_id = int(fund_name_from_file.split('_')[0])
    except ValueError:
        return None

    text = extract_text_from_pdf(pdf_path)
    if not text: return None

    benchmarks = set()

    # Broaden the trigger terminology for tables or paragraphs mentioning benchmarks
    triggers = r'(?:benchmark|graadmeter|referentie-index|referentie|normportefeuille|meetlat|beleggingscategorie)(.{0,500})'
    benchmark_windows = re.finditer(triggers, text)
    
    # Dramatically Expand the list of major index providers
    providers = r'(msci|ftse|bloomberg|barclays|dow jones|s&p|gbi|euribor|sonia|aex|eonia|iboxx|stoxx|gresb|j\.p\. morgan|jp morgan|citi|bofa|russell|refinitiv|solactive)'
    
    # Pattern 1: A benchmark name is usually "Provider [Words]" terminating in a specific index-related suffix
    index_pattern = re.compile(rf'({providers}\b[a-z0-9\-\s\(\)\+&,\.]+?(?:index|return|tr|nr|\b[0-9]%|hedged|unhedged|benchmark|maatstaf))')
    
    # Pattern 2: Naked string matches for major providers anywhere in text, tightly constrained by length
    strict_pattern = re.compile(rf'({providers}\b[a-z0-9\-\s\(\),\.]+?(?:index|return|benchmark|tr|nr))')

    for m in benchmark_windows:
        window = m.group(1)
        # Find specific names in the window context
        found = index_pattern.findall(window)
        for f in found:
            clean_name = f[0].strip().title()
            if 5 < len(clean_name) < 80:
                benchmarks.add(clean_name)

    # General sweep for highly specific names anywhere in the text
    general_found = strict_pattern.findall(text)
    for f in general_found:
        clean_name = f[0].strip().title()
        if len(clean_name) > 5 and len(clean_name) < 80:
            benchmarks.add(clean_name)

    benchmarks_str = ", ".join(sorted(list(benchmarks)))
    if not benchmarks_str:
        benchmarks_str = "None explicitly identified"
        
    return fund_id, benchmarks_str

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    pdf_files = [f for f in os.listdir(REPORTS_DIR) if f.endswith('.pdf')]
    print(f"Analyzing {len(pdf_files)} reports for investment benchmarks...")

    results = []
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = {executor.submit(analyze_benchmarks, f): f for f in pdf_files}
        
        for i, future in enumerate(as_completed(futures), 1):
            filename = futures[future]
            try:
                res = future.result()
                if res is not None:
                    fund_id, b_str = res
                    results.append((b_str, fund_id))
                    print(f"[{i}/{len(pdf_files)}] {filename} -> Benchmarks: {b_str[:80]}...")
            except Exception as e:
                print(f"Error processing {filename}: {e}")

    c.executemany("UPDATE funds SET benchmarks = ? WHERE id = ?", results)
    conn.commit()
    conn.close()
    
    print("Done! Benchmarks appended to database.")

if __name__ == "__main__":
    main()
