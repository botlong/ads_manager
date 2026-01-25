import sqlite3
import pandas as pd
import sys
import numpy as np

# Set output encoding to UTF-8
sys.stdout = open('product_analysis.txt', 'w', encoding='utf-8')

conn = sqlite3.connect('ads_data.sqlite')

columns = ['item_id', 'title', 'conv_value', 'cost', 'conversions', 'avg_order_value']
query = f"SELECT {', '.join(columns)} FROM product"

try:
    df = pd.read_sql(query, conn)
    
    # Calculate Margin: Conv. Value - Cost
    df['margin'] = df['conv_value'] - df['cost']
    
    # Calculate AOV: Conv. Value / Conversions (handle division by zero)
    df['calculated_aov'] = df.apply(
        lambda row: row['conv_value'] / row['conversions'] if row['conversions'] > 0 else 0, 
        axis=1
    )
    
    # Format columns for better readability
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    
    # Sort 1: By Item ID
    print("=== Sorted by Item ID (Top 5) ===")
    print(df[['item_id', 'title', 'margin', 'calculated_aov']].sort_values('item_id').head(5).to_string(index=False))
    print("\n")
    
    # Sort 2: By Margin (Desc)
    print("=== Sorted by Product Margin (Revenue - Cost) (Top 5) ===")
    print(df[['item_id', 'title', 'margin', 'calculated_aov']].sort_values('margin', ascending=False).head(5).to_string(index=False))
    print("\n")
    
    # Sort 3: By Calculated AOV (Desc)
    print("=== Sorted by Calculated AOV (Top 5) ===")
    print(df[['item_id', 'title', 'margin', 'calculated_aov']].sort_values('calculated_aov', ascending=False).head(5).to_string(index=False))
    
except Exception as e:
    print(f"Error: {e}")

conn.close()
