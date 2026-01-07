import pandas as pd
import mysql.connector
import logging
import re
from datetime import datetime

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO)

# ---------------- DATABASE CONFIG ----------------
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",  
    "database": "fleximart"
}

# ---------------- HELPER FUNCTIONS ----------------
def normalize_phone(phone):
    if pd.isna(phone):
        return None
    phone = re.sub(r"\D", "", str(phone))
    phone = phone[-10:]
    return f"+91-{phone}"

def parse_date(date_str):
    if pd.isna(date_str):
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(str(date_str), fmt).date()
        except ValueError:
            continue
    return None

def normalize_category(category):
    if pd.isna(category):
        return None
    return category.strip().title()

# ---------------- MAIN ETL ----------------
def main():
    logging.info("Starting ETL pipeline")

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    logging.info("Connected to MySQL database")

    # ---------------- CUSTOMERS ----------------
    customers = pd.read_csv("../data/customers_raw.csv")
    customers = customers.drop_duplicates()
    customers = customers[customers["email"].notna()]
    customers["phone"] = customers["phone"].apply(normalize_phone)
    customers["registration_date"] = customers["registration_date"].apply(parse_date)

    customer_insert = """
        INSERT INTO customers
        (first_name, last_name, email, phone, city, registration_date)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    for _, row in customers.iterrows():
        try:
            cursor.execute(customer_insert, (
                row["first_name"],
                row["last_name"],
                row["email"],
                row["phone"],
                row["city"],
                row["registration_date"]
            ))
        except mysql.connector.Error:
            pass

    conn.commit()

    # ---------------- PRODUCTS ----------------
    products = pd.read_csv("../data/products_raw.csv")
    products = products[products["price"].notna()]
    products["stock_quantity"] = products["stock_quantity"].fillna(0)
    products["category"] = products["category"].apply(normalize_category)

    product_insert = """
        INSERT INTO products
        (product_name, category, price, stock_quantity)
        VALUES (%s, %s, %s, %s)
    """

    for _, row in products.iterrows():
        try:
            cursor.execute(product_insert, (
                row["product_name"].strip(),
                row["category"],
                row["price"],
                int(row["stock_quantity"])
            ))
        except mysql.connector.Error:
            pass

    conn.commit()

    # ---------------- SALES ----------------
    sales = pd.read_csv("../data/sales_raw.csv")
    raw_sales = len(sales)

    # Remove duplicates
    sales = sales.drop_duplicates(subset=["transaction_id"])

    # Remove missing customer_id or product_id
    sales = sales[sales["customer_id"].notna() & sales["product_id"].notna()]

    # Normalize date
    sales["transaction_date"] = sales["transaction_date"].apply(parse_date)

    logging.info(f"Sales raw: {raw_sales}")
    logging.info(f"Sales cleaned: {len(sales)}")

    order_insert = """
        INSERT INTO orders (customer_id, order_date, total_amount, status)
        VALUES (%s, %s, %s, %s)
    """

    order_item_insert = """
        INSERT INTO order_items
        (order_id, product_id, quantity, unit_price, subtotal)
        VALUES (%s, %s, %s, %s, %s)
    """

    inserted_orders = 0

    for _, row in sales.iterrows():
        try:
            customer_id = int(row["customer_id"][1:])
            product_id = int(row["product_id"][1:])
            quantity = int(row["quantity"])
            unit_price = float(row["unit_price"])
            subtotal = quantity * unit_price

            # Insert order
            cursor.execute(order_insert, (
                customer_id,
                row["transaction_date"],
                subtotal,
                row["status"]
            ))

            order_id = cursor.lastrowid

            # Insert order item
            cursor.execute(order_item_insert, (
                order_id,
                product_id,
                quantity,
                unit_price,
                subtotal
            ))

            inserted_orders += 1

        except Exception as e:
            logging.warning(f"Skipping sale due to error: {e}")

    conn.commit()

    logging.info(f"Orders successfully loaded: {inserted_orders}")

    cursor.close()
    conn.close()

    logging.info("ETL pipeline COMPLETED successfully")

if __name__ == "__main__":
    main()
