import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os

db_path = r'c:\Users\WebkoWuite\DPS\VB Portefeuille Beheer - Documents\Zakelijke Waarden\Aandelen\EU equity\Research\Nederlandse-pensioenfondsen\data\pension_funds.db'
output_dir = 'plots'
output_path = os.path.join(output_dir, 'regional_exposure_py.png')

def plot_strategies():
    if not os.path.exists(db_path):
        print("Database not found.")
        return

    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT f.name, s.region, s.weight_pct 
    FROM funds f
    JOIN equity_strategies s ON f.id = s.fund_id
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        print("No strategy data found in database.")
        return

    # Clean fund names
    df['name'] = df['name'].str.replace(r' \(.*\)', '', regex=True)
    df['name'] = df['name'].str.replace('Stichting Pensioenfonds ', '')

    # Pivot for plotting
    pivot_df = df.pivot(index='name', columns='region', values='weight_pct')
    
    # Plotting
    plt.figure(figsize=(10, 6))
    pivot_df.plot(kind='barh', stacked=True, ax=plt.gca(), colormap='Set3')
    
    plt.title('Regional Equity Exposure - Top Dutch Pension Funds', fontweight='bold')
    plt.xlabel('Weight (%)')
    plt.ylabel('Pension Fund')
    plt.legend(title='Region', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    plt.savefig(output_path)
    print(f"Visualization saved to: {output_path}")

if __name__ == "__main__":
    plot_strategies()
