import requests
from bs4 import BeautifulSoup
import re

url = "https://www.abnamropensioenfonds.nl/over-ons/pensioenfonds/bestuur"
print(f"Testing {url}")
res = requests.get(url)
soup = BeautifulSoup(res.text, 'html.parser')
main = soup.find('main') or soup.body
text = main.get_text(separator='\n', strip=True)

for i, line in enumerate(text.split('\n')):
    if 'bestuurslid' in line.lower() or 'voorzitter' in line.lower() or 'directeur' in line.lower():
        start = max(0, i-2)
        end = min(len(text.split('\n')), i+3)
        print("--- Match block ---")
        for j in range(start, end):
            print(f"{'<--' if j==i else '   '} {text.split('\n')[j]}")

