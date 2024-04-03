import sqlite3
import csv

db_file = r'C:\Users\Zaid Chashoo\PycharmProjects\ASSIGNMENT\Data Engineer_ETL Assignment.db'
conn = sqlite3.connect(db_file)
cursor = conn.cursor()
cursor.execute("""
    SELECT
        c.customer_id,
        c.age,
        i.item_name,
        SUM(o.quantity) AS quantity
    FROM
        orders o
    JOIN
        sales s ON o.sales_id = s.sales_id
    JOIN
        items i ON o.item_id = i.item_id
    JOIN
        customers c ON s.customer_id = c.customer_id
    WHERE
        c.age BETWEEN 18 AND 35
    GROUP BY
        c.customer_id,
        c.age,
        i.item_name
    HAVING
        SUM(o.quantity) > 0
""")
csv_file_path = 'sql_final.csv'
rows = cursor.fetchall()


with open(csv_file_path, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['customer_id', 'age', 'item_name', 'quantity'])
    csv_writer.writerows(rows)

conn.close()
