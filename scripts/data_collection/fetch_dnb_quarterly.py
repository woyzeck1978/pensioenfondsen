"""Fetch DNB per-fund quarterly statistics JSON.

DNB dataset 8_18: "Gegevens individuele pensioenfondsen (Kwartaal)".
The legacy host statistiek.api.dnb.nl is dead; the new gateway is
api.dnb.nl/statpub-intapi-prd/v1/ which requires the public subscription
key that the DNB dashboard itself uses. The key is embedded in the
public dashboard JS — it is not a private credential.

Run from scripts/data_collection/ so the relative output path resolves.
"""

import os
from playwright.sync_api import sync_playwright

RESOURCE_ID = "a4b6584f-09b7-498d-bce5-3ef12e966f87"
URL = f"https://api.dnb.nl/statpub-intapi-prd/v1/resources/{RESOURCE_ID}/resourcefile/json"
SUBSCRIPTION_KEY = "e0249d4903b049e6844a8bc0c5961ddf"
OUT_PATH = "../../data/processed/dnb_per_fund_quarterly_raw.json"

def main():
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        api = browser.new_context().request
        print(f"GET {URL}")
        resp = api.get(
            URL,
            headers={"Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY, "Accept": "application/json"},
            timeout=180000,
        )
        if resp.status != 200:
            raise SystemExit(f"DNB API returned {resp.status}: {resp.body()[:200]!r}")
        body = resp.body()
        with open(OUT_PATH, "wb") as f:
            f.write(body)
        browser.close()
    size = os.path.getsize(OUT_PATH)
    print(f"  saved {size:,} bytes -> {OUT_PATH}")

if __name__ == "__main__":
    main()
