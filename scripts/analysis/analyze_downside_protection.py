import os
import sqlite3
import fitz
import re

DB_PATH = "../../data/processed/pension_funds.db"
REPORTS_DIR = "../data/reports"

# Keywords mapped to downside protection concepts
PROTECTION_KEYWORDS = [
    r'\\boptie\\b', r'\\bopties\\b', r'\\bswaptions\\b', r'\\bswaption\\b', r'\\bneerwaarts', 
    r'\\bdalingen\\b', r'\\bkoersdaling', r'\\bafdekken\\b', r'\\bafdekking\\b'
]
KEYWORD_PATTERN = re.compile('|'.join(PROTECTION_KEYWORDS), re.IGNORECASE)

def extract_protection(text):
    text = text.replace('\\n', ' ')
    sentences = re.split(r'(?<=[.!?]) +', text)
    
    matches = []
    
    for i, sentence in enumerate(sentences):
        lower_sent = sentence.lower()
        if KEYWORD_PATTERN.search(lower_sent):
            # Try to grab the surrounding sentence for context
            context_start = max(0, i - 1)
            context_end = min(len(sentences), i + 2)
            context = " ".join(sentences[context_start:context_end])
            
            # Simple heuristic to avoid generic privacy policies
            lower_context = context.lower()
            if 'persoons' in lower_context or 'privacy' in lower_context or 'avg' in lower_context:
                continue
            
            # Must mention something related to financial instruments or downside risk
            if 'optie' in lower_context or 'swaption' in lower_context or 'neerwaarts' in lower_context or 'risico' in lower_context or 'bescherm' in lower_context or 'afdek' in lower_context:
                context = re.sub(r'\\s+', ' ', context).strip()
                
                # filter out generic boring interest rate hedging if it doesn't mention options/cost
                if 'renterisico afgedekt' in lower_context and 'optie' not in lower_context and 'swaption' not in lower_context:
                    continue
                    
                if context not in matches:
                    matches.append(context)
                
            # If we find more than 4 instances, it's likely a generic document, let's limit to first 4 best matches
            if len(matches) >= 4:
                break
                
    if matches:
        return " | ".join(matches)
    return "Unknown"

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT id, name FROM funds WHERE annual_report_downloaded = 1")
    funds = c.fetchall()
    
    pdf_files = [f for f in os.listdir(REPORTS_DIR) if f.endswith('.pdf')]
    
    updates = []
    
    print(f"Scanning {len(pdf_files)} reports for downside protection strategies...")
    
    for i, file in enumerate(pdf_files):
        fund_id_str = file.split('_')[0]
        try:
            fund_id = int(fund_id_str)
        except ValueError:
            continue
            
        filepath = os.path.join(REPORTS_DIR, file)
        
        try:
            doc = fitz.open(filepath)
            # Only scan first 50 pages where investment strategy is usually discussed, to save time and reduce false positives in appendices
            num_pages = min(50, len(doc))
            
            full_text = ""
            for page_num in range(num_pages):
                page = doc.load_page(page_num)
                full_text += page.get_text()
                
            protection_context = extract_protection(full_text)
            print(f"[{i+1}/{len(pdf_files)}] {file} -> Strategy: {'Found' if protection_context != 'Unknown' else 'None'}")
            
            updates.append((protection_context, fund_id))
            
        except Exception as e:
            print(f"[{i+1}/{len(pdf_files)}] Error analyzing {file}: {e}")

    c.executemany("UPDATE funds SET downside_protection = ? WHERE id = ?", updates)
    conn.commit()
    conn.close()
    
    print("Done! Database updated with downside protection strategies.")

if __name__ == "__main__":
    main()
