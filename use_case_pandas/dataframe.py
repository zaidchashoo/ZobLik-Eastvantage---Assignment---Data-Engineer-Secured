import sqlite3
import pandas as pd
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
db_file = os.path.join(script_dir, 'Data Engineer_ETL Assignment.db')
csv_file_path = 'pd_final.csv'
try:
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    df_sales = pd.read_sql_query("SELECT * FROM  sales", conn)
    df_customers = pd.read_sql_query("SELECT * FROM  customers", conn)
    df_orders = pd.read_sql_query("SELECT * FROM  orders", conn)
    df_items = pd.read_sql_query("SELECT * FROM  items", conn)

    df_customers = df_customers[df_customers['age'].isin(list(range(18,36)))]
    df_merged = pd.merge(df_orders, df_sales, on='sales_id', how='inner')
    df_merged = pd.merge(df_merged, df_items, on='item_id', how='inner')
    df_merged = pd.merge(df_merged, df_customers, on='customer_id', how='inner')
    df_grouped = df_merged.groupby(['customer_id', 'item_name','age']).agg({'quantity': 'sum'}).reset_index()
    df_grouped = df_grouped[df_grouped['quantity'] > 0]
    df_grouped['quantity'] = df_grouped['quantity'].astype(int)
    df_final = df_grouped[['customer_id', 'age', 'item_name', 'quantity']]

    print(df_final)
    df_final.to_csv('pd_final.csv', index=False)

except sqlite3.Error as e:
    print("SQLite error:", e)

except IOError as e:
    print("I/O error:", e)

except Exception as e:
    print("Error occurred:", e)

finally:
    if 'conn' in locals():
        conn.close()
