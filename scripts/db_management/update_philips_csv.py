import csv

lines = []
found = False
with open("../../data/processed/extracted_allocations.csv", "r") as f:
    reader = csv.reader(f)
    for row in reader:
        if row and row[0] == "119":
            row[1] = "22.80"
            found = True
        lines.append(row)

if not found:
    lines.append(["119", "22.80", "119_Philips.pdf"])

with open("../../data/processed/extracted_allocations.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(lines)
print("Updated CSV for Philips.")
