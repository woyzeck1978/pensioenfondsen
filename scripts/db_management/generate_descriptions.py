import sqlite3

db_path = "data/processed/pension_funds.db"

def generate_description(name, category, aum, participants):
    desc = ""
    # Determine the type
    if category == 'Beroep':
        desc += f"Pensioenfonds {name} is een Nederlands beroepspensioenfonds."
    elif category == 'Tak':
        desc += f"Pensioenfonds {name} is een Nederlands bedrijfstakpensioenfonds."
    elif category == 'Onderneming':
        desc += f"Pensioenfonds {name} is een Nederlands ondernemingspensioenfonds."
    elif category == 'APF':
        desc += f"{name} is een Algemeen Pensioenfonds (APF) in Nederland."
    elif category == 'PPI':
        desc += f"{name} is een Premiepensioeninstelling (PPI) in Nederland."
    else:
        desc += f"Pensioenfonds {name} is een pensioenuitvoerder in Nederland."

    # Add size context
    size_parts = []
    if aum and float(aum) > 0:
        if float(aum) >= 1.0:
            size_parts.append(f"een beheerd vermogen van circa €{float(aum):.1f} miljard")
        else:
            size_parts.append(f"een beheerd vermogen van circa €{float(aum)*1000:.0f} miljoen")
            
    if participants and int(participants) > 0:
        size_parts.append(f"verzorgt de pensioenen voor ongeveer {int(participants):,} deelnemers".replace(',', '.'))
        
    if size_parts:
        if len(size_parts) == 2:
            desc += f" Het fonds heeft {size_parts[0]} en {size_parts[1]}."
        else:
            if "vermogen" in size_parts[0]:
                desc += f" Het fonds heeft {size_parts[0]}."
            else:
                desc += f" Het fonds {size_parts[0]}."
                
    return desc

try:
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # Get all funds
    c.execute("SELECT id, name, category, aum_euro_bn, deelnemers_totaal FROM funds")
    funds = c.fetchall()
    
    updated_count = 0
    for fund in funds:
        fid, name, category, aum, participants = fund
        desc = generate_description(name, category, aum, participants)
        c.execute("UPDATE funds SET description = ? WHERE id = ?", (desc, fid))
        updated_count += 1
        
    conn.commit()
    print(f"Successfully generated and updated descriptions for {updated_count} funds.")

except Exception as e:
    print(f"Error: {e}")
finally:
    if conn:
        conn.close()
