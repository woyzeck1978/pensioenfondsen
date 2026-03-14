import sqlite3
import time
from ddgs import DDGS
import argparse
import pandas as pd
from urllib.parse import urlparse
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_missing_corporate_funds(db_path, limit=None):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = "SELECT id, name FROM funds WHERE website IS NULL"
    if limit:
        query += f" LIMIT {limit}"
    cursor.execute(query)
    funds = cursor.fetchall()
    conn.close()
    return funds

BAD_DOMAINS = [
    'wikipedia.org', 'dnb.nl', 'pensioenfederatie.nl', 'companyinfo.nl',
    'kvk.nl', 'youtube.com', 'facebook.com', 'linkedin.com', 'overheid.nl',
    'werkenbij', 'vacatures', 'rtlnieuws', 'nu.nl', 'nrc.nl', 'fd.nl',
    'volkskrant', 'telegraaf', 'nos.nl', 'rijksoverheid.nl', 'toezicht.dnb.nl',
    'radar.avrotros.nl', 'wijzeringeldzaken.nl', 'pensioenklokkenluider.nl',
    'pensioenkijker.nl', 'pensioenduidelijkheid.nl', 'bing.com', 'google.com',
    'glassdoor.', 'indeed.'
]

def score_url(url, fund_name):
    url_lower = url.lower()
    
    # Check if bad domain
    for bd in BAD_DOMAINS:
        if bd in url_lower:
            return -100
            
    if 'pdf' in url_lower or 'nieuws' in url_lower:
        return -50
        
    score = 0
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    
    # Bonus for .nl
    if domain.endswith('.nl'):
        score += 10
        
    # Bonus if 'pensioen' in domain
    if 'pensioen' in domain:
        score += 20
        
    # Bonus if fund name (simplified) in domain
    fund_words = [w for w in fund_name.lower().replace('-', ' ').replace('/', ' ').replace('(', '').replace(')', '').replace('&', '').split() if len(w) > 2]
    # Filter out generic words
    generic = ['nederland', 'stichting', 'pensioenfonds', 'algemeen', 'personeel', 'en']
    specific_words = [w for w in fund_words if w not in generic]
    
    for sw in specific_words:
        if sw in domain:
            score += 30
            
    # Very short path is preferred for official sites (e.g. site.nl/ vs site.nl/news/article)
    if parsed.path in ['', '/', '/nl', '/home']:
        score += 15
    else:
        # Penalty for deep links
        score -= len(parsed.path.split('/')) * 2
        
    return score

def search_website(fund_name):
    clean_name = fund_name.replace('/', ' ').replace('(', '').replace(')', '').replace('&', '')
    clean_name = ' '.join(clean_name.split())
    
    queries = [
        f'"{clean_name}" pensioenfonds',
        f'pensioenfonds {clean_name}',
        f'stichting pensioenfonds {clean_name}'
    ]
    
    best_url = None
    best_score = 0  # Require strictly positive score to be a valid website
    
    for q in queries:
        print(f"  Query: {q}")
        try:
            results = DDGS().text(q, max_results=10)
            if not results:
                continue
            for r in results:
                href = r.get('href', '')
                score = score_url(href, fund_name)
                if score > best_score:
                    best_score = score
                    best_url = href
                    
            if best_score >= 30:
                print(f"  => Early exit with strong score ({best_score})")
                break
        except Exception as e:
            print(f"  Error with DDGS: {e}")
            break
            
        time.sleep(3) # Wait between queries
        
    return best_url, best_score

def main():
    parser = argparse.ArgumentParser(description="Find missing websites for corporate pension funds")
    parser.add_argument("--limit", type=int, help="Limit the number of funds to process")
    args = parser.parse_args()

    db_path = '../../data/processed/pension_funds.db'

    funds = get_missing_corporate_funds(db_path, limit=args.limit)
    print(f"Found {len(funds)} corporate funds missing websites.")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    success_count = 0

    for i, (fund_id, fund_name) in enumerate(funds):
        print(f"\n[{i+1}/{len(funds)}] Processing [{fund_id}] {fund_name}...")
        
        url, score = search_website(fund_name)
        
        if url and score > 0:
            print(f"  => Found URL (Score {score}): {url}")
            success_count += 1
            cursor.execute('UPDATE funds SET website = ? WHERE id = ?', (url, fund_id))
            conn.commit()
        else:
            print(f"  => No suitable website found. (Best score: {score} for {url})")
            
        # Give duckduckgo some breathing room
        time.sleep(4)

    conn.close()
    print(f"\nDone! Found websites for {success_count}/{len(funds)} missing corporate funds.")

if __name__ == "__main__":
    main()
