
"""
Script: extract_top_spenders.py
Description: Extracts top 10 spenders per month from a large CSV file using pandas.
"""

import pandas as pd

INPUT_FILE = "large_order_data_2024.csv"
OUTPUT_FILE = "top_10_spenders_per_month.xlsx"
CHUNKSIZE = 100000

monthly_data = {}

for chunk in pd.read_csv(INPUT_FILE, chunksize=CHUNKSIZE):
    chunk['month'] = pd.to_datetime(chunk['transaction_datetime']).dt.month
    grouped = chunk.groupby(['month', 'customer_no'])['amount'].sum().reset_index()

    for month in grouped['month'].unique():
        month_df = grouped[grouped['month'] == month]
        if month in monthly_data:
            monthly_data[month] = pd.concat([monthly_data[month], month_df])
        else:
            monthly_data[month] = month_df

final_top_spenders = {}
for month, df in monthly_data.items():
    aggregated = df.groupby('customer_no')['amount'].sum().reset_index()
    top_10 = aggregated.sort_values(by='amount', ascending=False).head(10)
    final_top_spenders[month] = top_10

with pd.ExcelWriter(OUTPUT_FILE) as writer:
    for month, df in final_top_spenders.items():
        df.to_excel(writer, sheet_name=f"Month_{month:02d}", index=False)

print("Top 10 spenders per month exported successfully.")
