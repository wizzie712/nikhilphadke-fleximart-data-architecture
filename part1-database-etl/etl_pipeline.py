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
            pass  # Ignore duplicates on rerun

    conn.commit()
    logging.info(f"Customers loaded: {len(customers)}")

    # ---------------- PRODUCTS ----------------
    products = pd.read_csv("../data/products_raw.csv")
    raw_products = len(products)

    # Remove products with missing price
    products = products[products["price"].notna()]

    # Fill missing stock with 0
    products["stock_quantity"] = products["stock_quantity"].fillna(0)

    # Normalize category names
    products["category"] = products["category"].apply(normalize_category)

    logging.info(f"Products raw: {raw_products}")
    logging.info(f"Products cleaned: {len(products)}")

    product_insert = """
        INSERT INTO products
        (product_name, category, price, stock_quantity)
        VALUES (%s, %s, %s, %s)
    """

    inserted_products = 0

    for _, row in products.iterrows():
        try:
            cursor.execute(product_insert, (
                row["product_name"].strip(),
                row["category"],
                row["price"],
                int(row["stock_quantity"])
            ))
            inserted_products += 1
        except mysql.connector.Error as e:
            logging.warning(f"Skipping product: {e}")

    conn.commit()
    logging.info(f"Products successfully loaded: {inserted_products}")

    cursor.close()
    conn.close()
    logging.info("Product ETL completed successfully")

if __name__ == "__main__":
    main()
