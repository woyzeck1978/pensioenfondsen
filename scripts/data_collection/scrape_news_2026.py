import sqlite3
import re
from playwright.sync_api import sync_playwright, TimeoutError
import concurrent.futures

DB_PATH = "../../data/processed/pension_funds.db"

def extract_toeslag(text, year=2026):
    lines = text.split('\n')
    actieven_pct = None
    gewezen_pct = None
    
    for i in range(len(lines)):
        if str(year) in lines[i]:
            window = ' '.join(lines[max(0, i-4):min(len(lines), i+5)]).lower()
            if 'toeslag' in window or 'indexatie' in window or 'verhoging' in window \
               or 'verhogen' in window or 'pensioenen gaan omhoog' in window:
                
                pcts = re.findall(r'(\d+[,.]\d+)\s*%', window)
                if not pcts:
                    continue
                    
                # Simplest fallback: if there's only one percentage, it probably applies to both
                if len(pcts) == 1:
                    val = pcts[0].replace(',', '.')
                    return val, val

                if 'actiev' in window or 'opbouwend' in window:
                    m = re.search(r'(actiev|opbouwend)[^\d%]*?(\d+[,.]\d+)\s*%', window)
                    if m: actieven_pct = m.group(2).replace(',', '.')
                    else:
                        m2 = re.search(r'(\d+[,.]\d+)\s*%[^\d%]*?(actiev|opbouwend)', window)
                        if m2: actieven_pct = m2.group(1).replace(',', '.')
                        
                if 'gewezen' in window or 'gepensioneerd' in window:
                    m = re.search(r'(gewezen|gepensioneerd)[^\d%]*?(\d+[,.]\d+)\s*%', window)
                    if m: gewezen_pct = m.group(2).replace(',', '.')
                    else:
                        m2 = re.search(r'(\d+[,.]\d+)\s*%[^\d%]*?(gewezen|gepensioneerd)', window)
                        if m2: gewezen_pct = m2.group(1).replace(',', '.')
                        
                if actieven_pct or gewezen_pct:
                    return actieven_pct, gewezen_pct
                    
    return None, None

def process_news_url(url, context):
    act_26, gew_26 = None, None
    try:
        page = context.new_page()
        page.goto(url, timeout=15000, wait_until='domcontentloaded')
        text = page.locator('body').inner_text()
        act_26, gew_26 = extract_toeslag(text, 2026)
        page.close()
    except Exception as e:
        pass
    return act_26, gew_26

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    query = """
    SELECT DISTINCT f.id, f.name
    FROM funds f
    JOIN scraped_documents s ON f.id = s.fund_id
    WHERE (f.toeslag_actieven_2026_pct IS NULL OR f.toeslag_gewezen_2026_pct IS NULL)
    AND s.doc_type = 'news'
    """
    c.execute(query)
    target_funds = c.fetchall()
    print(f"Funds to check news articles for 2026 indexation: {len(target_funds)}")
    
    updates = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0")
        
        for fund_id, name in target_funds:
            c.execute("SELECT url FROM scraped_documents WHERE fund_id = ? AND doc_type = 'news' ORDER BY discovered_at DESC LIMIT 10", (fund_id,))
            urls = [row[0] for row in c.fetchall()]
            
            found_act = None
            found_gew = None
            
            for url in urls:
                act_26, gew_26 = process_news_url(url, context)
                if act_26 and not found_act: found_act = act_26
                if gew_26 and not found_gew: found_gew = gew_26
                if found_act and found_gew: break
                
            if found_act or found_gew:
                print(f"Fund {name} -> 2026 Toeslag Actieven: {found_act}%, Gewezen: {found_gew}%")
                
                upd_q = []
                params = []
                if found_act:
                    upd_q.append("toeslag_actieven_2026_pct = ?")
                    params.append(float(found_act))
                if found_gew:
                    upd_q.append("toeslag_gewezen_2026_pct = ?")
                    params.append(float(found_gew))
                    
                params.append(fund_id)
                updates.append((f"UPDATE funds SET {', '.join(upd_q)} WHERE id = ?", params))

        browser.close()

    for q, p in updates:
        c.execute(q, p)
        
    conn.commit()
    conn.close()
    print(f"Successfully discovered 2026 indexation for {len(updates)} missing funds.")

if __name__ == "__main__":
    main()
