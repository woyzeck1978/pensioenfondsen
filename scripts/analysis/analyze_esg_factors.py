import os
import sqlite3
import fitz  # PyMuPDF
import re
import argparse

def extract_text_from_pdf(filepath):
    try:
        doc = fitz.open(filepath)
        text = ""
        for page in doc:
            text += page.get_text()
        return text
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

def find_esg_and_factor_style(text):
    if not text:
        return None, None
        
    text = text.lower()
    
    # ESG analysis
    esg_keywords = ['esg', 'duurzaamheid', 'duurzaam', 'maatschappelijk verantwoord', 'mvo', 'sri', 'uitsluitingen', 'klimaat', 'co2']
    esg_score = 0
    
    # Factor analysis
    factor_keywords = ['factorbeleggen', 'factor investing', 'smart beta', 'value', 'momentum', 'quality', 'kwaliteit', 'low volatility', 'lage volatiliteit', 'size', 'factoren']
    factor_score = 0
    
    for kw in esg_keywords:
        esg_score += len(re.findall(r'\b' + re.escape(kw) + r'\b', text))
        
    for kw in factor_keywords:
        # For general words like 'value' and 'quality', we need to be more careful. 
        # Only count them if they appear near 'factor' or 'smart beta' or 'aandelen'
        if kw in ['value', 'momentum', 'quality', 'kwaliteit', 'size']:
            for match in re.finditer(r'\b' + re.escape(kw) + r'\b', text):
                start = max(0, match.start() - 100)
                end = min(len(text), match.end() + 100)
                window = text[start:end]
                if 'factor' in window or 'smart beta' in window or 'aandelen' in window or 'premie' in window:
                    factor_score += 1
        else:
             factor_score += len(re.findall(r'\b' + re.escape(kw) + r'\b', text))
             
    # Determine ESG Focus
    esg_focus = 'Standard'
    if esg_score > 50:
        esg_focus = 'High (Extensive ESG/Sustainability Focus)'
    elif esg_score > 10:
        esg_focus = 'Medium (Mentions ESG/Exclusions)'
    
    # Negative checks for factor investing
    if 'geen factorbeleggen' in text or 'geen factoren' in text or 'niet in factoren' in text:
        factor_investing = 'No'
    else:
        # Determine Factor Investing
        factor_investing = 'Unknown'
        if factor_score > 15:
            factor_investing = 'Yes (Strong Factor/Smart Beta Focus)'
        elif factor_score > 3:
            factor_investing = 'Yes (Mentions Factor Investing)'
        elif factor_score == 0:
            factor_investing = 'No (No specific mention)'
        
    return esg_focus, factor_investing


def main():
    parser = argparse.ArgumentParser(description="Analyze downloaded annual reports for ESG and Factor Investing styles")
    parser.add_argument("--limit", type=int, help="Limit the number of reports to process")
    args = parser.parse_args()

    reports_dir = '../data/reports'
    db_path = '../../data/processed/pension_funds.db'

    if not os.path.exists(reports_dir):
        print(f"Directory not found: {reports_dir}. Run the download script first.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    files = [f for f in os.listdir(reports_dir) if f.endswith('.pdf')]
    print(f"Found {len(files)} reports to analyze for ESG and Factor Investing.")

    if args.limit:
        files = files[:args.limit]

    count_updated = 0
    
    for i, filename in enumerate(files):
        print(f"\n[{i+1}/{len(files)}] Analyzing {filename}...")
        
        try:
            fund_id = int(filename.split('_')[0])
        except ValueError:
            print(f"Skipping {filename}: Could not extract fund ID.")
            continue

        filepath = os.path.join(reports_dir, filename)
        text = extract_text_from_pdf(filepath)
        
        esg, factor = find_esg_and_factor_style(text)
        
        if esg or factor:
            print(f"Found ESG: {esg} | Factor: {factor}")
            
            # Check if this fund exists in the equity_strategy table
            cursor.execute("SELECT id FROM equity_strategy WHERE fund_id = ?", (fund_id,))
            exists = cursor.fetchone()
            
            if exists:
                cursor.execute('''
                UPDATE equity_strategy
                SET esg_focus = ?, factor_investing = ?
                WHERE fund_id = ?
                ''', (esg, factor, fund_id))
            else:
                cursor.execute('''
                INSERT INTO equity_strategy (fund_id, esg_focus, factor_investing)
                VALUES (?, ?, ?)
                ''', (fund_id, esg, factor))
                
            conn.commit()
            count_updated += 1
        else:
            print("Could not find ESG/Factor details in text.")

    conn.close()
    print(f"\nDone! Extracted ESG/Factor allocations for {count_updated}/{len(files)} reports.")

if __name__ == "__main__":
    main()
