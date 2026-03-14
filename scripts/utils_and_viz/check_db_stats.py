import sqlite3

conn = sqlite3.connect('data/processed/pension_funds.db')
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM funds")
total = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM funds WHERE status LIKE '%Open%' OR status LIKE '%Gesloten%' OR status LIKE '%Ingevaren%'")
active = cursor.fetchone()[0]

print(f"Total Funds: {total}")
print(f"Active/Tracked Funds: {active}")
print("\n=== Completeness for Active Funds ===")

cols = [
    'aum_euro_bn',
    'equity_allocation_pct',
    'dekkingsgraad_pct',
    'beleidsdekkingsgraad_pct',
    'deelnemers_totaal',
    'toeslag_actieven_2025_pct',
    'transitieplan_url'
]

for col in cols:
    cursor.execute(f"SELECT COUNT(*) FROM funds WHERE {col} IS NOT NULL AND (status LIKE '%Open%' OR status LIKE '%Gesloten%' OR status LIKE '%Ingevaren%')")
    filled = cursor.fetchone()[0]
    missing = active - filled
    pct = (filled / active) * 100
    print(f"*   **{col}**: {filled}/{active} filled ({pct:.1f}%) | {missing} missing")

conn.close()
