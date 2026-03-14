import pandas as pd
import sqlite3

print("--- DB COLUMNS ---------")
conn = sqlite3.connect('../../data/processed/pension_funds.db')
cursor = conn.execute('SELECT * FROM funds LIMIT 1')
print([description[0] for description in cursor.description])
conn.close()

dnb = pd.read_excel('../data/DNB Gegevens individuele pensioenfondsen 2023-2025.xlsx', header=1)
print("\n--- DNB COLUMNS ---------")
for i, col in enumerate(dnb.columns):
    print(f"{i}: {col}")

wtp = pd.read_excel('../data/Overzicht pensioentransitie wtp.xlsx', header=4)
print("\n--- WTP COLUMNS ---------")
for i, col in enumerate(wtp.columns):
    print(f"{i}: {col}")
