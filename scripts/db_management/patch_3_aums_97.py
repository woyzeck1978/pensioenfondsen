import sqlite3

DB_PATH = "../../data/processed/pension_funds.db"

# Format: (fund_name_like, new_aum_bn)
CORRECTIONS = [
    ('Avery Dennison', 0.457),
    ('Citigroup', 0.119),
    ('Lloyd%Register', 0.270)
]

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    for name, aum in CORRECTIONS:
        c.execute("UPDATE funds SET aum_euro_bn = ? WHERE name LIKE ?", (aum, f"%{name}%"))
        print(f"Updated {name} to {aum} bn")
        
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
