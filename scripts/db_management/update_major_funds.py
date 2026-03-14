import sqlite3
import os

db_path = r'c:\Users\WebkoWuite\DPS\VB Portefeuille Beheer - Documents\Zakelijke Waarden\Aandelen\EU equity\Research\Nederlandse-pensioenfondsen\data\pension_funds.db'

def update_top_funds():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Data gathered from research
    updates = [
        ("ABP", 533.0, 29.5, "Q3 2024 data. Equity allocation 29.5% (Listed). Decreased from Q1 slightly.", 2024, "Sector"),
        ("PFZW", 259.0, None, "2024 data. Invested assets €259bn.", 2024, "Sector"),
        ("PMT", 84.4, None, "Q1 2024 data. Invested assets €84.4bn. High private equity allocation (~11%).", 2024, "Sector"),
        ("BPFBouw", 69.5, 31.2, "2024 data. Invested assets €69.5bn. Equity (Shares) 24.3% + PE 6.9%.", 2024, "Sector"),
        ("PME", 59.9, None, "2024 data. Invested assets €59.9bn. Implementing New ESG framework.", 2024, "Sector"),
    ]

    for name_part, aum, equity_pct, notes, year, category in updates:
        # Search for the name in the DB (might be slightly different like 'Stichting Pensioenfonds ABP')
        cursor.execute("SELECT id, name FROM funds WHERE name LIKE ?", (f"%{name_part}%",))
        result = cursor.fetchone()
        
        if result:
            fund_id, full_name = result
            cursor.execute('''
            UPDATE funds 
            SET aum_euro_bn = ?, equity_allocation_pct = ?, equity_strategy_notes = ?, last_report_year = ?, category = ?
            WHERE id = ?
            ''', (aum, equity_pct, notes, year, category, fund_id))
            print(f"Updated {full_name}")
        else:
            # Insert if not found
            cursor.execute('''
            INSERT INTO funds (name, aum_euro_bn, equity_allocation_pct, equity_strategy_notes, last_report_year, category, data_source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (name_part, aum, equity_pct, notes, year, category, "Manual Research"))
            print(f"Inserted {name_part}")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    update_top_funds()
