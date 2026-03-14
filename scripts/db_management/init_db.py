import sqlite3
import os

db_path = r'c:\Users\WebkoWuite\DPS\VB Portefeuille Beheer - Documents\Zakelijke Waarden\Aandelen\EU equity\Research\Nederlandse-pensioenfondsen\data\pension_funds.db'

def init_db():
    if os.path.exists(db_path):
        print(f"Database already exists at {db_path}. No changes made.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create funds table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS funds (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        category TEXT,
        aum_euro_bn REAL,
        equity_allocation_pct REAL,
        equity_strategy_notes TEXT,
        last_report_year INTEGER,
        website TEXT,
        data_source TEXT
    )
    ''')

    # Create a table for tracking specific strategy details if needed later
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS equity_strategies (
        fund_id INTEGER,
        region TEXT,
        weight_pct REAL,
        FOREIGN KEY (fund_id) REFERENCES funds (id)
    )
    ''')

    conn.commit()
    conn.close()
    print(f"Initialized database at {db_path}")

if __name__ == "__main__":
    init_db()
