import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os

db_path = '../../data/processed/pension_funds.db'
output_dir = 'plots'
output_path = os.path.join(output_dir, 'regional_exposure_all.png')

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

def plot_all_strategies():
    if not os.path.exists(db_path):
        print("Database not found.")
        return

    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT f.name, f.aum_euro_bn, s.geographic_allocation, s.explicit_geographic_weights
    FROM funds f
    JOIN equity_strategy s ON f.id = s.fund_id
    WHERE s.geographic_allocation IS NOT NULL OR s.explicit_geographic_weights IS NOT NULL
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        print("No strategy data found in database.")
        return

    # Clean fund names
    df['name'] = df['name'].str.replace(r' \(.*\)', '', regex=True)
    df['name'] = df['name'].str.replace('Stichting Pensioenfonds ', '')
    df['name'] = df['name'].str.replace('Pensioenfonds ', '')
    
    # Sort by AUM descending so biggest are at the top
    df = df.sort_values(by='aum_euro_bn', ascending=True)

    rows = []
    for _, row in df.iterrows():
        fund = row['name']
        geo = row['geographic_allocation']
        explicit_geo = row.get('explicit_geographic_weights', None)
        
        # Prefer explicit weights if available
        if explicit_geo and isinstance(explicit_geo, str) and explicit_geo.strip():
            allocations = parse_explicit_weights(explicit_geo)
        else:
            allocations = parse_geo_allocation(geo)
            
        for region, weight in allocations:
            rows.append({
                'name': fund,
                'region': region,
                'weight_pct': weight
            })

    expanded_df = pd.DataFrame(rows)
    
    if expanded_df.empty:
        print("No parsable geographic regions found.")
        return

    # Pivot for plotting
    pivot_df = expanded_df.pivot(index='name', columns='region', values='weight_pct')
    
    # Ensuring the order matches AUM sorted df order
    pivot_df = pivot_df.reindex(df['name'])
    
    # Optional sorting: sort by highest North America allocation for a nicer curve
    if 'North America' in pivot_df.columns:
        pivot_df = pivot_df.sort_values(by='North America')
    
    # Plotting
    # Make the figure height dynamic based on the number of funds
    fig_height = max(10, len(pivot_df) * 0.25)
    plt.figure(figsize=(10, fig_height)) # Width changed to 10 to match old script
    
    pivot_df.plot(kind='barh', stacked=True, ax=plt.gca(), colormap='Set3')
    
    plt.title('Regional Equity Exposure - All Dutch Pension Funds', fontweight='bold')
    plt.xlabel('Weight (%)')
    plt.ylabel('Pension Fund')
    plt.legend(title='Region', bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Enforce 0-100 x-axis
    plt.xlim(0, 100)
    
    plt.tight_layout()

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    plt.savefig(output_path, dpi=300) # Matched DPI and format
    print(f"Visualization saved to: {output_path}")

if __name__ == "__main__":
    plot_all_strategies()
