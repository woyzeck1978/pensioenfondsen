import os
import sqlite3
import fitz  # PyMuPDF
import re

DB_PATH = "data/processed/pension_funds.db"
REPORTS_DIR = "data/reports"

# Keywords that indicate the start of the investment beliefs section
KEYWORDS = [
    "beleggingsovertuiging",
    "investment belief",
    "beleggingsbeginsel"
]

def clean_text(text):
    # Remove excessive newlines and spaces
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def extract_beliefs(pdf_path):
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        print(f"Error opening {pdf_path}: {e}")
        return None
        
    for page_num in range(len(doc)):
        page = doc[page_num]
        text = page.get_text()
        text_lower = text.lower()
        
        for kw in KEYWORDS:
            idx = text_lower.find(kw)
            if idx != -1:
                # Find start of the sentence or paragraph
                start_idx = max(0, text_lower.rfind('\n', 0, idx))
                # Extract up to 2500 characters
                end_idx = min(len(text), idx + 2500)
                extracted = text[start_idx:end_idx]
                
                # Try to cut off at the next major section if possible, else just use the chunk
                # A simple heuristic: find the next double newline
                next_section_idx = extracted.find('\n\n\n')
                if next_section_idx > 500: # Ensure we capture something
                     extracted = extracted[:next_section_idx]
                     
                doc.close()
                return clean_text(extracted)
                
    doc.close()
    return None

def main():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all funds
    cursor.execute("SELECT id, name FROM funds")
    funds = cursor.fetchall()
    
    updated_count = 0
    
    for fund_id, fund_name in funds:
        # Construct expected PDF filename
        # Based on previous pattern, often "ID_Name.pdf" or we just search the dir
        # Let's search the directory for any file starting with ID_
        prefix = f"{fund_id}_"
        matched_file = None
        for filename in os.listdir(REPORTS_DIR):
            if filename.startswith(prefix) and filename.endswith(".pdf"):
                matched_file = os.path.join(REPORTS_DIR, filename)
                break
                
        if matched_file:
            print(f"Processing {matched_file} for {fund_name}...")
            beliefs_text = extract_beliefs(matched_file)
            
            if beliefs_text:
                print(f"  -> Found investment beliefs ({len(beliefs_text)} chars). Updating db...")
                cursor.execute("UPDATE funds SET investment_beliefs = ? WHERE id = ?", (beliefs_text, fund_id))
                updated_count += 1
            else:
                print("  -> No investment beliefs found.")
        else:
            # Optionally check if there's a file by name
            pass
            
    conn.commit()
    conn.close()
    print(f"Done. Updated {updated_count} funds.")

if __name__ == "__main__":
    main()
