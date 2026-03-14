import sqlite3
import pandas as pd
import os

db_path = '../../data/processed/pension_funds.db'

def view_data():
    if not os.path.exists(db_path):
        print("Database not found.")
        return

    conn = sqlite3.connect(db_path)
    
    print("\n--- Summary of Funds Table ---")
    df = pd.read_sql_query("SELECT id, name, category, aum_euro_bn, equity_allocation_pct FROM funds", conn)
    
    # Show top funds and a random sample
    print(df.sort_values(by="aum_euro_bn", ascending=False).head(10))
    
    print(f"\nTotal funds in database: {len(df)}")
    conn.close()

if __name__ == "__main__":
    view_data()
