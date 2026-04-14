import pandas as pd
import glob

csv_files = sorted(glob.glob('orders/EtsySoldOrders*.csv'))

dfs = []
for csv_file in csv_files:
    df = pd.read_csv(csv_file, usecols=['Sale Date', 'Ship City', 'Ship State', 'Ship Country'])
    dfs.append(df)
    print(f"Read {len(df)} rows from {csv_file.split('/')[-1]}")

combined = pd.concat(dfs, ignore_index=True)
combined.columns = ['sale_date', 'city', 'state', 'country']

output_path = 'orders/consolidated_sales.csv'
combined.to_csv(output_path, index=False)

print(f"\nConsolidated {len(combined)} total rows -> consolidated_sales.csv")
