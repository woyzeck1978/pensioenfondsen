import sqlite3

db_path = "data/processed/pension_funds.db"

def add_column():
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if the column already exists
        cursor.execute("PRAGMA table_info(funds)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if "investment_beliefs" not in columns:
            cursor.execute("ALTER TABLE funds ADD COLUMN investment_beliefs TEXT")
            print("Successfully added 'investment_beliefs' column to 'funds' table.")
        else:
            print("'investment_beliefs' column already exists in 'funds' table.")
            
        conn.commit()
    except Exception as e:
        print(f"Error checking/adding column: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    add_column()
