import requests
from bs4 import BeautifulSoup
import urllib3
import re

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

URLS = {
    'NN CDC': 'https://nn.cdcpensioen.nl/nieuws/dekkingsgraden-per-september-2025-zijn-bekend',
    'Witteveen+Bos': 'https://pensioenfonds.witteveenbos.nl/nl/nieuw/dekkingsgraad_dec_2025/',
    'KLM Cabinefonds': 'https://klmcabinefonds.nl/nieuws/dekkingsgraad-december-2025-1324',
    'Stap GE': 'https://www.stappensioen.nl/pensioenkring-ge/financiele-situatie',
    'ExxonMobil': 'https://www.exxonmobilofp.nl/over-pensioenfonds-exxonmobil-ofp/financiele-situatie/',
    'PostNL': 'https://www.pensioenpostnl.nl/financiele-situatie',
    'PPF APG': 'https://www.ppf-apg.nl/over-ppf-apg/nieuws/2025/we-zijn-over-op-de-nieuwe-regels',
    'Recreatie': 'https://www.pensioenfondsrecreatie.nl/j',
    'Bisdommen Kerncijfers': 'https://www.pnb.nl/over-het-fonds/actuele-cijfers/kerncijfers-per-jaar/'
}

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

for name, url in URLS.items():
    print(f"\n{'='*50}\nFund: {name}\nURL: {url}")
    try:
        r = requests.get(url, headers=headers, verify=False, timeout=10)
        soup = BeautifulSoup(r.content, 'html.parser')
        
        snippets = []
        for p in soup.find_all(['p', 'div', 'h1', 'h2', 'h3', 'li', 'td', 'th']):
            text = p.get_text().strip().replace('\n', ' ')
            text = re.sub(' +', ' ', text)
            
            # Look for keywords related to what we need
            keywords = ['dekkingsgraad', 'vermogen', 'milj', '€', 'overdracht', 'invaren', '%']
            has_keyword = any(kw in text.lower() for kw in keywords)
            has_number = any(char.isdigit() for char in text)
            
            if has_keyword and has_number:
                if 10 < len(text) < 400:
                    if text not in snippets:
                        snippets.append(text)
        
        if snippets:
            for s in snippets[:20]:  # Limit output to first 20 matches
                print(f" -> {s}")
        else:
            raw_text = soup.get_text().replace('\n', ' ')
            raw_text = re.sub(' +', ' ', raw_text)
            print(" -> No relevant numeric snippets found. Showing raw text sample:")
            print(raw_text[:500])
            
    except Exception as e:
        print(f" -> Error: {e}")
