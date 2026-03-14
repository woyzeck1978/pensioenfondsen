import os
import sqlite3
import fitz
import re

DB_PATH = "../../data/processed/pension_funds.db"
REPORTS_DIR = "../data/reports"

def find_toeslag(year, text):
    lines = text.split('\n')
    actieven_pct = None
    gewezen_pct = None
    
    for i in range(len(lines)):
        if str(year) in lines[i]:
            window = ' '.join(lines[max(0, i-5):min(len(lines), i+6)]).lower()
            if 'toeslag' in window or 'indexatie' in window or 'verhoging' in window:
                pcts = re.findall(r'(\d+[,.]\d+)\s*%', window)
                if not pcts:
                    continue
                    
                if 'actiev' in window or 'opbouwend' in window:
                    m = re.search(r'(actiev|opbouwend)[^\d%]*?(\d+[,.]\d+)\s*%', window)
                    if m:
                        actieven_pct = m.group(2).replace(',', '.')
                    else:
                        m2 = re.search(r'(\d+[,.]\d+)\s*%[^\d%]*?(actiev|opbouwend)', window)
                        if m2:
                            actieven_pct = m2.group(1).replace(',', '.')
                            
                if 'gewezen' in window or 'gepensioneerd' in window:
                    m = re.search(r'(gewezen|gepensioneerd)[^\d%]*?(\d+[,.]\d+)\s*%', window)
                    if m:
                        gewezen_pct = m.group(2).replace(',', '.')
                    else:
                        m2 = re.search(r'(\d+[,.]\d+)\s*%[^\d%]*?(gewezen|gepensioneerd)', window)
                        if m2:
                            gewezen_pct = m2.group(1).replace(',', '.')
                            
                if actieven_pct or gewezen_pct:
                    return actieven_pct, gewezen_pct
                    
    return actieven_pct, gewezen_pct

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT id, name FROM funds WHERE name NOT LIKE '%Avery%'")
    funds = c.fetchall()
    
    for fund_id, name in funds:
        pdf_path = None
        for f in os.listdir(REPORTS_DIR):
            if f.startswith(f"{fund_id}_") and f.endswith(".pdf"):
                pdf_path = os.path.join(REPORTS_DIR, f)
                break
                
        if not pdf_path:
            continue
            
        try:
            doc = fitz.open(pdf_path)
            # Scan the first 30 pages usually containing Vorwoord and Kerncijfers
            text = '\n'.join([p.get_text() for p in doc[:30]])
            
            act_25, gew_25 = find_toeslag(2025, text)
            act_26, gew_26 = find_toeslag(2026, text)
            
            updates = []
            params = []
            if act_25:
                updates.append("toeslag_actieven_2025_pct = ?")
                params.append(float(act_25))
            if gew_25:
                updates.append("toeslag_gewezen_2025_pct = ?")
                params.append(float(gew_25))
            if act_26:
                updates.append("toeslag_actieven_2026_pct = ?")
                params.append(float(act_26))
            if gew_26:
                updates.append("toeslag_gewezen_2026_pct = ?")
                params.append(float(gew_26))
                
            if updates:
                params.append(fund_id)
                query = f"UPDATE funds SET {', '.join(updates)} WHERE id = ?"
                c.execute(query, params)
                print(f"Updated {name}: 2025({act_25}, {gew_25}) | 2026({act_26}, {gew_26})")
                
        except Exception as e:
            pass
            
    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()
