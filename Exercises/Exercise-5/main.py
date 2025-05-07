import psycopg2
import csv
from datetime import datetime

def create_tables(cur):
    # Đọc và thực thi schema.sql để tạo bảng
    with open("schema.sql", "r") as f:
        cur.execute(f.read())

def insert_csv_data(cur, conn):
    # Insert accounts.csv
    try:
        with open("data/accounts.csv", "r") as f:
            reader = csv.DictReader(f, skipinitialspace=True)
            for row in reader:
                # Kiểm tra nếu giá trị trống thì gán là None
                address_2 = row['address_2'] if row['address_2'] else None
                cur.execute("""
                    INSERT INTO accounts VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    int(row['customer_id']), row['first_name'], row['last_name'],
                    row['address_1'], address_2, row['city'],
                    row['state'], row['zip_code'],
                    datetime.strptime(row['join_date'], '%Y/%m/%d').date()
                ))
    except FileNotFoundError:
        print("Error: The file 'accounts.csv' does not exist.")
        return

    # Insert products.csv
    try:
        with open("data/products.csv", "r") as f:
            reader = csv.DictReader(f, skipinitialspace=True)
            for row in reader:
                cur.execute("""
                    INSERT INTO products VALUES (%s, %s, %s)
                """, (int(row['product_id']), row['product_code'], row['product_description']))
    except FileNotFoundError:
        print("Error: The file 'products.csv' does not exist.")
        return

    # Insert transactions.csv
    try:
        with open("data/transactions.csv", "r") as f:
            reader = csv.DictReader(f, skipinitialspace=True)
            for row in reader:
                cur.execute("""
                    INSERT INTO transactions VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['transaction_id'],
                    datetime.strptime(row['transaction_date'], '%Y/%m/%d').date(),
                    int(row['product_id']),
                    row['product_code'],
                    row['product_description'],
                    int(row['quantity']),
                    int(row['account_id'])
                ))
    except FileNotFoundError:
        print("Error: The file 'transactions.csv' does not exist.")
        return

    # Commit tất cả các thay đổi
    conn.commit()

def main():
    # Sử dụng context manager để tự động đóng kết nối và con trỏ
    try:
        with psycopg2.connect(
            host="postgres",
            database="postgres",
            user="postgres",
            password="postgres"
        ) as conn:
            with conn.cursor() as cur:
                create_tables(cur)  # Tạo bảng
                insert_csv_data(cur, conn)  # Nhập dữ liệu từ các file CSV
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
