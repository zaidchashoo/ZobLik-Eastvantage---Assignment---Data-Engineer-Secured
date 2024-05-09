import sqlite3
import csv

db_file = r'C:\Users\Zaid Chashoo\PycharmProjects\ASSIGNMENT\Data Engineer_ETL Assignment.db'
csv_file_path = 'sql_final.csv'

try:
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    sql_query = """
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
    """

    cursor.execute(sql_query)
    rows = cursor.fetchall()
    print (rows)

except sqlite3.Error as e:
    print("SQLite error:", e)

except IOError as e:
    print("I/O error:", e)

except Exception as e:
    print("Error occurred:", e)

finally:
    rows = []  # Initialize rows to an empty list
    with open(csv_file_path, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['customer_id', 'age', 'item_name', 'quantity'])
        csv_writer.writerows(rows)

    if 'conn' in locals():
        conn.close()
        print("CSV file created successfully.")
