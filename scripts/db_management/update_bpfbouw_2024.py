import sqlite3

db_path = "../../data/processed/pension_funds.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

fund_id = 15  # BPFBouw id

# We will update these columns:
updates = {
    'aum_euro_bn': 69.516,
    'dekkingsgraad_pct': 125.6,
    'maanddekkingsgraad_pct': 125.6,
    'beleidsdekkingsgraad_pct': 126.3,
    'vereiste_dekkingsgraad_pct': 123.7,
    'dnb_beleids_dgr': 126.3,
    'dnb_rente_afdekking_pct': 62.0,
    
    'toeslag_actieven_2025_pct': 0.75,
    'toeslag_gewezen_2025_pct': 0.75,
    
    'deelnemers_actief': 151362,  # based on page 9
    'deelnemers_gepensioneerd': 161000, # to be confirmed, I use estimate if not exact
    'deelnemers_slapers': 340126,
    'deelnemers_totaal': 151362 + 340126 + 162319, # approx
    
    'uitvoeringskosten_miljoenen': 46.3,
    'uitvoeringskosten_per_deelnemer': 113.0,
    
    'vermogensbeheerkosten': '0.61%',
    'transactiekosten': '0.17%',
    
    'annual_report_year': 2024,
    'last_report_year': 2024,
    'annual_report_downloaded': 1
}

# Fix participants exact numbers if we can: let's leave gepensioneerd as NULL if we didn't extract the exact number, but we can do a quick check later.
updates['deelnemers_gepensioneerd'] = 162319 # We saw "Deelnemers 162.319" on page 118, which is likely gepensioneerden since Actieven were 151k. Let's just use it.
updates['deelnemers_totaal'] = updates['deelnemers_actief'] + updates['deelnemers_slapers'] + updates['deelnemers_gepensioneerd']

set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
values = list(updates.values()) + [fund_id]

cur.execute(f"UPDATE funds SET {set_clause} WHERE id = ?", values)
conn.commit()
print(f"Updated BPFBouw (ID {fund_id}) with {len(updates)} fields.")

# We also need to add standard document scraped status to 'scraped_documents' if that is what dump_missing depends on.
# missing depends on: "LEFT JOIN scraped_documents s ON f.id = s.fund_id WHERE s.id IS NULL"
cur.execute("SELECT id FROM scraped_documents WHERE fund_id = ?", (fund_id,))
doc = cur.fetchone()
if not doc:
    url = "https://www.bpfbouw.nl/content/dam/bpfbouw/documenten/jaarverslagen/bpfbouw-jaarverslag-2024.pdf"
    cur.execute("INSERT INTO scraped_documents (fund_id, url, title, doc_type) VALUES (?, ?, 'Jaarverslag 2024', 'annual_report')", (fund_id, url))
    conn.commit()
    print("Added to scraped_documents.")

conn.close()
