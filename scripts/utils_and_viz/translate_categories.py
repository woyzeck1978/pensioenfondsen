import sqlite3

db_path = '../../data/processed/pension_funds.db'
conn = sqlite3.connect(db_path)
c = conn.cursor()

# Map English to Dutch
mapping = {
    'Corporate': 'Bedrijf',
    'Occupational': 'Beroep',
    'Industry-wide': 'Tak',
    'Sector': 'Tak',
    'tak': 'Tak',
    'General': 'APF',
    'Pension Insurers (Pensioenverzekeraars)': 'Verzekeraar',
    'Pension Insurers': 'Verzekeraar'
}

c.execute("SELECT DISTINCT category FROM funds WHERE category IS NOT NULL")
current_cats = [row[0] for row in c.fetchall()]

for cat in current_cats:
    if cat in mapping:
        c.execute("UPDATE funds SET category = ? WHERE category = ?", (mapping[cat], cat))
    elif 'APF' in cat and cat != 'APF':
        # Leave as is if it's "Algemeen Pensioenfonds (Kring)" or something specific
        pass

conn.commit()
conn.close()
print("Categories successfully translated to Dutch shorthand!")
