import csv

lines = []
with open("../../data/processed/extracted_allocations.csv", "r") as f:
    reader = csv.reader(f)
    for row in reader:
        if row and row[0] == "71":
            row[1] = "17.00"
        lines.append(row)

with open("../../data/processed/extracted_allocations.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(lines)
print("Updated CSV.")
