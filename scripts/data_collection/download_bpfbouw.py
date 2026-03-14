import requests
from pypdf import PdfReader
import io
import os

url = "https://www.bpfbouw.nl/content/dam/bpfbouw/documenten/jaarverslagen/bpfbouw-jaarverslag-2024.pdf"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/pdf'
}

print(f"Downloading {url}...")
response = requests.get(url, headers=headers)
response.raise_for_status()

pdf_path = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen/data/bpfbouw-jaarverslag-2024.pdf"

with open(pdf_path, 'wb') as f:
    f.write(response.content)

print(f"Saved PDF to {pdf_path}")

reader = PdfReader(pdf_path)
num_pages = len(reader.pages)
print(f"Total pages: {num_pages}")


# We will just write the script to download it first. 
