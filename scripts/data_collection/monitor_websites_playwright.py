import sqlite3
import argparse
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

def get_db_connection():
    conn = sqlite3.connect('../../data/processed/pension_funds.db')
    conn.row_factory = sqlite3.Row
    return conn

def extract_links_from_page(page, url, base_domain):
    """Extract and categorize links using javascript evaluation in the browser."""
    discovered = []
    
    # We execute JS directly in the browser to grab all current <a> tags
    # This ensures we get them even if they were rendered dynamically by React/Angular
    links = page.evaluate('''() => {
        return Array.from(document.querySelectorAll('[href]')).map(el => {
            let text = el.innerText ? el.innerText.trim() : '';
            if (!text && el.textContent) text = el.textContent.trim();
            if (!text) text = el.title || '';
            if (!text && el.querySelector && el.querySelector('img')) text = el.querySelector('img').alt || '';
            let href = el.href || el.getAttribute('href');
            return { href: href, text: text };
        });
    }''')
    
    for link in links:
        href = link.get('href')
        text = link.get('text')
        if not href:
            continue
            
        full_url = urljoin(url, href)
        parsed = urlparse(full_url)
        
        if base_domain not in parsed.netloc:
            continue
            
        doc_type = None
        
        # 1. Detect PDFs and Documents
        if full_url.lower().endswith(('.pdf', '.docx', '.xlsx', '.doc')):
            doc_type = 'document'
        # 2. Detect News/Actueel articles
        elif ('/nieuws/' in full_url.lower() or '/actueel/' in full_url.lower()) and full_url != url:
            path_parts = [p for p in parsed.path.split('/') if p]
            if len(path_parts) > 1:
                doc_type = 'news'
                
        if doc_type:
            discovered.append({
                'url': full_url,
                'title': text,
                'doc_type': doc_type
            })
            
    return discovered

def scan_fund_playwright(fund_id, fund_name, website_url, context, conn):
    """Scrapes a specific fund's website using Playwright."""
    if not website_url or website_url == "None":
        return []
        
    print(f"\n[Playwright] Scanning {fund_name} ({website_url})")
    
    if not website_url.startswith('http'):
        website_url = 'https://' + website_url
        
    base_domain = urlparse(website_url).netloc
    
    paths_to_check = [
        '/',
        '/nieuws',
        '/documenten',
        '/documenten/'
    ]
    
    if 'bpfbouw.nl' in base_domain:
        paths_to_check.extend(['/over-bpfbouw/ons-financieel-beleid/jaarverslagen', '/over-bpfbouw/nieuws'])
    elif 'bplpensioen.nl' in base_domain:
        paths_to_check.extend(['/financiele-situatie', '/publicaties-en-documenten'])
    elif 'befrank.nl' in base_domain:
        paths_to_check.extend(['/over-ons/jaarverslag/', '/nieuws/'])
    elif 'pensioenfondscampina.nl' in base_domain:
        paths_to_check.extend(['/over-ons/financiele-situatie/'])
    elif 'centraalbeheerapf.nl' in base_domain:
        paths_to_check.extend(['/over-centraal-beheer-apf/jaarverslagen', '/werkgever/pensioenregeling/dekkingsgraden'])
    elif 'pdnpensioen.nl' in base_domain:
        paths_to_check.extend(['/nl/downloads'])
    elif 'pensioenfondshal.nl' in base_domain:
        paths_to_check.extend(['/downloads/jaarverslag/'])
    elif 'sphn.nl' in base_domain:
        paths_to_check.extend(['/documenten/jaarverslagen/', '/documenten/nieuwsbrieven/'])
    elif 'phenc.nl' in base_domain:
        paths_to_check.extend(['/info/downloads'])
    elif 'klmcabinefonds.nl' in base_domain:
        paths_to_check.extend(['/pensioenregeling/documenten'])
    elif 'bpfl.nl' in base_domain:
        paths_to_check.extend(['/documenten'])
    elif 'pmepensioen.nl' in base_domain:
        paths_to_check.extend(['/publicaties-pme'])
    elif 'pmepensioen.nl' in base_domain:
        paths_to_check.extend(['/publicaties-pme'])
    elif 'denationaleapf.nl' in base_domain:
        paths_to_check.extend(['/deelnemers/nieuws/', '/deelnemers/over-het-fonds/financi%C3%ABle-positie/', '/deelnemers/documenten/'])
    elif 'oakpensioenfonds.nl' in base_domain:
        paths_to_check.extend(['/juridische-informatie-en-beleid'])
    elif 'pob.eu' in base_domain:
        paths_to_check.extend(['/documenten', '/nieuwsoverzicht'])
    elif 'sprh.nl' in base_domain:
        paths_to_check.extend(['/dekkingsgraad/', '/nieuws/', '/downloads-2/'])
    elif 'pensioenschoonmaak.nl' in base_domain:
        paths_to_check.extend([
            '/over-pensioenschoonmaak/nieuws', 
            '/over-pensioenschoonmaak/ons-financieel-beleid/financiele-documenten',
            '/over-pensioenschoonmaak/ons-financieel-beleid/jaarverslag'
        ])
    elif 'pensioenvoldoen.nl' in base_domain:
        paths_to_check.extend([
            '/over-stichting-pensioenvoldoen/',
            '/pensioenfonds/stichting-pensioenfonds-tobacon-offshore-marine-consultancy-b-v/'
        ])
    elif 'unileverpensioenfonds.nl' in base_domain:
        paths_to_check.extend(['/forward/', '/forward/pensioenregeling/indexatie-beleid/dekkingsgraad/', '/forward/documenten/fondsdocumenten/'])
    elif 'pwri.nl' in base_domain:
        paths_to_check.extend(['/over-pwri/nieuws', '/over-pwri/ons-financieel-beleid/jaarverslagen'])
    elif 'spw.nl' in base_domain:
        paths_to_check.extend([
            '/over-spw/ons-financieel-beleid', 
            '/over-spw/ons-financieel-beleid/jaarverslagen',
            '/over-spw/ons-financieel-beleid/indexatiebeleid'
        ])
    elif 'kikk-recreatie.nl' in base_domain:
        paths_to_check.extend(['/cao-pensioen/cao-recreatie/'])
        
    all_discovered = []
    visited = set()
    
    page = context.new_page()
    
    for path in paths_to_check:
        target_url = urljoin(website_url, path)
        if target_url in visited: continue
        visited.add(target_url)
        
        # Some generic aggregator sites aren't worth deep waiting
        if 'startpagina.nl' in target_url:
            continue
            
        try:
            # wait_until='networkidle' is the magic bullet for React/Angular sites
            page.goto(target_url, wait_until='networkidle', timeout=15000)
            
            # Dismiss simple cookie banners if they obstruct clicks (optional, but good practice)
            # We don't strictly need to click them just to read the DOM, but sometimes it helps rendering
            # Dismiss cookie banners much more aggressively
            try:
                # Wait specifically for the page to settle after initial load
                page.wait_for_timeout(2000)
                
                page.evaluate('''() => {
                    const buttons = Array.from(document.querySelectorAll('button, a, input[type="submit"]'));
                    // Common Dutch cookie acceptance phrases
                    const acceptTexts = ['akkoord', 'accepteer', 'begrepen', 'ja, ik ga akkoord', 'alles accepteren', 'toestaan'];
                    
                    for (const btn of buttons) {
                        const text = (btn.innerText || btn.value || '').toLowerCase();
                        if (acceptTexts.some(t => text.includes(t))) {
                            btn.click();
                            break; // Click the first one we find and exit
                        }
                    }
                }''')
                page.wait_for_timeout(2000) # Give it 2 seconds to vanish and the real DOM to load
            except Exception:
                pass
                
            links = extract_links_from_page(page, target_url, base_domain)
            all_discovered.extend(links)
            
        except PlaywrightTimeoutError:
            print(f"  -> Timeout accessing {target_url}")
        except Exception as e:
            print(f"  -> Error accessing {target_url}: {e}")
            
    page.close()
    
    # Deduplicate within this single run
    unique_discovered = {}
    for item in all_discovered:
        unique_discovered[item['url']] = item
        
    new_findings = []
    cursor = conn.cursor()
    
    for item in unique_discovered.values():
        url = item['url']
        
        cursor.execute("SELECT id FROM scraped_documents WHERE url = ?", (url,))
        existing = cursor.fetchone()
        
        if not existing:
            try:
                cursor.execute(
                    "INSERT INTO scraped_documents (fund_id, url, title, doc_type) VALUES (?, ?, ?, ?)",
                    (fund_id, url, item['title'], item['doc_type'])
                )
                conn.commit()
                new_findings.append(item)
            except sqlite3.IntegrityError:
                pass
                
    return new_findings

def main():
    parser = argparse.ArgumentParser(description="Playwright Pension Fund Web Scraper (For JS-Heavy Sites)")
    parser.add_argument('--test', action='store_true', help="Run in test mode on just 3 funds")
    args = parser.parse_args()

    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Query ONLY funds that currently have 0 documents scraped
    query = """
    SELECT f.id, f.name, f.website 
    FROM funds f 
    LEFT JOIN scraped_documents s ON f.id = s.fund_id 
    WHERE s.id IS NULL 
    AND f.website IS NOT NULL 
    AND f.website != '' 
    AND f.website != 'None'
    ORDER BY f.name
    """
    
    if args.test:
        query += " LIMIT 3"
        
    cursor.execute(query)
    funds = cursor.fetchall()
    
    if len(funds) == 0:
        print("No missing funds found! The database is fully populated.")
        conn.close()
        return
        
    print(f"Starting Playwright scan of {len(funds)} MISSING pension fund websites...")
    
    total_new_docs = 0
    total_new_news = 0
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # Set a realistic user agent
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )
        
        for fund in funds:
            try:
                new_items = scan_fund_playwright(fund['id'], fund['name'], fund['website'], context, conn)
                
                if new_items:
                    print(f"  *** PLAYWRIGHT FOUND {len(new_items)} NEW ITEMS for {fund['name']}! ***")
                    for item in new_items:
                        if args.test:
                            print(f"    - [{item['doc_type'].upper()}] {(item['title'] or '')[:60]}: {item['url']}")
                        if item['doc_type'] == 'document': total_new_docs += 1
                        elif item['doc_type'] == 'news': total_new_news += 1
            except Exception as e:
                print(f"Failed to scan {fund['name']}: {e}")
                
        browser.close()
            
    print("\n========================================================")
    print("PLAYWRIGHT SCAN COMPLETE.")
    print(f"Total New Documents Rescued: {total_new_docs}")
    print(f"Total New News Articles Rescued: {total_new_news}")
    print("========================================================")
    
    conn.close()

if __name__ == "__main__":
    main()
