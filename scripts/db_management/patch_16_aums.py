import sqlite3

DB_PATH = "../../data/processed/pension_funds.db"

# Format: (fund_name_like, new_aum_bn)
CORRECTIONS = [
    ('AT&T Nederland', 0.164),
    ('Brocacef', 0.355),
    ('Delta Lloyd', 2.346),
    ('Essity', 0.607),
    ('HAL', 0.143),
    ('Mercer', 0.136),
    ('Metro', 1.054), # Using 2023 AUM prior to liquidation
    ('Pon', 0.435),
    ('Provisum', 1.454),
    ('Rockwool', 1.420),
    ('SABIC', 2.910),
    ('Sagittarius', 0.324),
    ('Smurfit Kappa', 0.875),
    ('Ecolab', 0.129),
    ('Exxonmobil', 2.894),
    ('GE Artesia', 0.226)
]

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Apply standard corrections
    for name, aum in CORRECTIONS:
        c.execute("UPDATE funds SET aum_euro_bn = ? WHERE name LIKE ?", (aum, f"%{name}%"))
        print(f"Updated {name} to {aum} bn")
        
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
