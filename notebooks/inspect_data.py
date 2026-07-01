import sqlite3
import pandas as pd

conn = sqlite3.connect('/Users/bohan/Downloads/novacart_project/delta-lakehouse-pipeline/data/landing/products.db')
df = pd.read_sql("SELECT * FROM products", conn)
df.to_csv('/Users/bohan/Downloads/novacart_project/delta-lakehouse-pipeline/data/landing/products/products.csv', index=False)

print(f"Exported {len(df)} rows to products.csv")
conn.close()