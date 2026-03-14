import sqlite3
import pandas as pd
import os

db_path = '../../data/processed/pension_funds.db'

def parse_explicit_weights(explicit_str):
    if not explicit_str:
        return []
    
    result = []
    # format: "North America: 65.0, Europe: 15.0"
    parts = [p.strip() for p in explicit_str.split(',')]
    total = 0
    for p in parts:
        if ':' in p:
            region, weight_str = p.split(':', 1)
            try:
                weight = float(weight_str.strip())
                region = region.strip()
                col_name = 'Other' if region == 'Pacific/Other' else region
                result.append((col_name, weight))
                total += weight
            except ValueError:
                pass
                
    # Normalize if it's vastly off 100%
    if total > 0 and (total < 90 or total > 110):
        normalized_result = []
        for region, weight in result:
            normalized_result.append((region, (weight / total) * 100.0))
        return normalized_result
        
    return result

def parse_geo_allocation(geo_str):
    if not geo_str:
        return []
    
    parts = [p.strip() for p in geo_str.split(',')]
    if not parts:
        return []
        
    STANDARD_WEIGHTS = {
        'North America': 63.0,
        'Europe': 16.0,
        'Emerging Markets': 11.0,
        'Other': 10.0,
        'Netherlands': 1.0,
    }
    
    # If Global or all 3 main regions are referenced, apply standard global MSCI ACWI weights
    if 'Global' in parts or {'North America', 'Europe', 'Emerging Markets'}.issubset(set(parts)):
        return [
            ('North America', 63.0),
            ('Europe', 16.0),
            ('Emerging Markets', 11.0),
            ('Other', 10.0)
        ]
        
    # Otherwise, apply proportional market weights to only the regions mentioned
    total_weight = sum(STANDARD_WEIGHTS.get(p, 10.0) for p in parts)
    
    result = []
    for p in parts:
        w = STANDARD_WEIGHTS.get(p, 10.0)
        norm_weight = (w / total_weight) * 100.0
        # If it was Pacific/Other, map to "Other" to match the old legend
        col_name = 'Other' if p == 'Pacific/Other' else p
        result.append((col_name, norm_weight))
        
    return result

def update_table():
    if not os.path.exists(db_path):
        print("Database not found.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = """
    SELECT f.id as fund_id, s.geographic_allocation, s.explicit_geographic_weights 
    FROM funds f
    JOIN equity_strategy s ON f.id = s.fund_id
    WHERE s.geographic_allocation IS NOT NULL OR s.explicit_geographic_weights IS NOT NULL
    """
    df = pd.read_sql_query(query, conn)
    
    # Clear the existing table to re-populate it totally
    cursor.execute("DELETE FROM equity_strategies")
    
    count = 0
    for _, row in df.iterrows():
        fund_id = row['fund_id']
        geo = row['geographic_allocation']
        explicit_geo = row.get('explicit_geographic_weights', None)
        
        if explicit_geo and isinstance(explicit_geo, str) and explicit_geo.strip():
            allocations = parse_explicit_weights(explicit_geo)
        else:
            allocations = parse_geo_allocation(geo)
            
        for region, weight in allocations:
            # Round out the weights for neatness in Excel
            weight = round(weight, 2)
            cursor.execute('''
            INSERT INTO equity_strategies (fund_id, region, weight_pct)
            VALUES (?, ?, ?)
            ''', (fund_id, region, weight))
            count += 1
            
    conn.commit()
    conn.close()
    
    print(f"Successfully populated 'equity_strategies' table with {count} total rows.")

if __name__ == "__main__":
    update_table()
