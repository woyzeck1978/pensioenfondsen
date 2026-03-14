import sqlite3
import os

db_path = r'c:\Users\WebkoWuite\DPS\VB Portefeuille Beheer - Documents\Zakelijke Waarden\Aandelen\EU equity\Research\Nederlandse-pensioenfondsen\data\pension_funds.db'

def populate_strategies():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Regional weightings (approximate based on 2023/2024 research)
    # North America, Europe (incl. NL), Emerging Markets, Asia/Other
    strategies = [
        ("ABP", [
            ("North America", 45.0),
            ("Europe", 25.0),
            ("Emerging Markets", 15.0),
            ("Asia Pacific", 15.0)
        ]),
        ("PFZW", [
            ("North America", 40.0),
            ("Europe", 35.0),
            ("Emerging Markets", 10.0),
            ("Other", 15.0)
        ]),
        ("BPFBouw", [
            ("North America", 50.0),
            ("Europe", 30.0),
            ("Emerging Markets", 10.0),
            ("Other", 10.0)
        ]),
        ("PMT", [
            ("North America", 48.0),
            ("Europe", 32.0),
            ("Emerging Markets", 12.0),
            ("Other", 8.0)
        ]),
        ("PME", [
            ("North America", 46.0),
            ("Europe", 34.0),
            ("Emerging Markets", 11.0),
            ("Other", 9.0)
        ])
    ]

    # Clear existing strategies for these funds to avoid duplicates on re-run
    for fund_name, _ in strategies:
        cursor.execute("DELETE FROM equity_strategies WHERE fund_id IN (SELECT id FROM funds WHERE name LIKE ?)", (f"%{fund_name}%",))

    for fund_name, regions in strategies:
        cursor.execute("SELECT id FROM funds WHERE name LIKE ?", (f"%{fund_name}%",))
        result = cursor.fetchone()
        
        if result:
            fund_id = result[0]
            for region, weight in regions:
                cursor.execute('''
                INSERT INTO equity_strategies (fund_id, region, weight_pct)
                VALUES (?, ?, ?)
                ''', (fund_id, region, weight))
            print(f"Populated strategy for {fund_name}")
        else:
            print(f"Fund {fund_name} not found in database.")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    populate_strategies()
