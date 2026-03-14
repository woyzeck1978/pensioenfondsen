import csv
import sqlite3

lines = []
with open("../../data/processed/extracted_allocations.csv", "r") as f:
    reader = csv.reader(f)
    for row in reader:
        if row and row[0] == "106":
            row[1] = "38.50"
        lines.append(row)

with open("../../data/processed/extracted_allocations.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(lines)

conn = sqlite3.connect("../../data/processed/pension_funds.db")
cursor = conn.cursor()
cursor.execute("UPDATE equity_allocations_extracted SET allocation_pct=38.5 WHERE fund_id=106;")
conn.commit()
conn.close()
print("Updated CSV and DB for Hoogovens.")
