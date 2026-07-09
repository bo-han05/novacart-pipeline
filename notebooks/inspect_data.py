import sqlite3
import json
import pandas as pd

## Databricks cannot read SQlite .db files natively, convert to csv

# conn = sqlite3.connect("/Users/bohan/Downloads/novacart_project/novacart-pipeline/data/landing/products/products.db")
# df = pd.read_sql("SELECT * FROM products", conn)
# df.to_csv("/Users/bohan/Downloads/novacart_project/novacart-pipeline/data/landing/products/products.csv", index=False)

# print(f"Exported {len(df)} rows to products.csv")
# conn.close()

with open("/Users/bohan/Downloads/novacart_project/novacart-pipeline/data/landing/customers/customers.json") as f:
    data = json.load(f)

print("=== CUSTOMERS ===")
print(type(data))
print(json.dumps(data[0], indent=2))

print("\n=== ORDERS ===")
df = pd.read_csv("/Users/bohan/Downloads/novacart_project/novacart-pipeline/data/landing/orders/orders_2025-11-10.csv")
print(df.columns.tolist())
print(df.head())

print("\n=== PRODUCTS ===")
df = pd.read_csv("/Users/bohan/Downloads/novacart_project/novacart-pipeline/data/landing/products/products.csv")
print(df.columns.tolist())
print(df.head())