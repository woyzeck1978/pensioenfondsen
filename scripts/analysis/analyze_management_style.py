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

def find_management_style(text):
    if not text:
        return None
        
    text = text.lower()
    
    # We want to find sections discussing active vs passive management
    keywords = ['actief beheer', 'passief', 'indexvolgend', 'tracking error', 'benchmark']
    
    style_mentions = {'Active': 0, 'Passive': 0}
    
    # Ahold Delhaize specifically mentions: "Binnen de aandelenportefeuille wordt vanaf 2019 niet meer actief belegd."
    if 'niet meer actief belegd' in text or 'niet actief belegd' in text or 'geen actief beheer' in text:
        style_mentions['Passive'] += 5  # Strong signal for passive
    if 'volledig passief' in text or 'geheel passief' in text or 'uitsluitend passief' in text:
        style_mentions['Passive'] += 5
        
    for keyword in keywords:
        for match in re.finditer(keyword, text):
            # Look at a window before and after
            start = max(0, match.start() - 200)
            end = min(len(text), match.end() + 200)
            window = text[start:end]
            
            # Context checking for equity
            if 'aandelen' in window or 'equity' in window or 'zakelijke waarden' in window:
                if 'actief beheer' in window or 'actieve' in window or 'actief' in window:
                    # Check for negations
                    if 'geen actief' in window or 'niet actief' in window or 'niet meer actief' in window or '0% van de technische voorzieningen' in window or 'uitsluitend passief' in window:
                        style_mentions['Passive'] += 2
                    else:
                        style_mentions['Active'] += 1
                        
                if 'passief' in window or 'indexvolgend' in window or 'tracker' in window or 'passieve' in window:
                    # Check for partial applicability
                    if 'deels passief' in window:
                        style_mentions['Passive'] += 1
                        style_mentions['Active'] += 1
                    else:
                        style_mentions['Passive'] += 1
                        
    if style_mentions['Passive'] == 0 and style_mentions['Active'] == 0:
        return None
        
    if style_mentions['Passive'] > style_mentions['Active'] * 1.5:
        return 'Passive'
    elif style_mentions['Active'] > style_mentions['Passive'] * 1.5:
        return 'Active'
    elif style_mentions['Active'] > 0 and style_mentions['Passive'] > 0:
        return 'Mixed (Active & Passive)'
    
    # Default fallback if slightly skewed but not strictly matching ratio
    return 'Passive' if style_mentions['Passive'] >= style_mentions['Active'] else 'Active'


def main():
    parser = argparse.ArgumentParser(description="Analyze downloaded annual reports for active vs passive equity management styles")
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
    print(f"Found {len(files)} reports to analyze for management style.")

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
        
        style = find_management_style(text)
        
        if style:
            print(f"Found management style: {style}")
            
            # Check if this fund exists in the equity_strategy table
            cursor.execute("SELECT id FROM equity_strategy WHERE fund_id = ?", (fund_id,))
            exists = cursor.fetchone()
            
            if exists:
                cursor.execute('''
                UPDATE equity_strategy
                SET management_style = ?
                WHERE fund_id = ?
                ''', (style, fund_id))
            else:
                cursor.execute('''
                INSERT INTO equity_strategy (fund_id, management_style)
                VALUES (?, ?)
                ''', (fund_id, style))
                
            conn.commit()
            count_updated += 1
        else:
            print("Could not definitively find management style details in text.")

    conn.close()
    print(f"\nDone! Extracted management style allocations for {count_updated}/{len(files)} reports.")

if __name__ == "__main__":
    main()
