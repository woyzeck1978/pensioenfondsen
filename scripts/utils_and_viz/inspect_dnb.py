import pandas as pd

try:
    dnb = pd.read_excel('../data/DNB Gegevens individuele pensioenfondsen 2023-2025.xlsx', header=None)
    print("--- DNB TOP ROWS ---")
    print(dnb.head(15).to_string())

    wtp = pd.read_excel('../data/Overzicht pensioentransitie wtp.xlsx', header=None)
    print("\n--- WTP TOP ROWS ---")
    print(wtp.head(15).to_string())
except Exception as e:
    print(e)
