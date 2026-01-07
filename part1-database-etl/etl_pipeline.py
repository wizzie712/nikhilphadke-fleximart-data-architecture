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

# ---------------- MAIN ETL ----------------
def main():
    logging.info("Starting ETL pipeline")

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    logging.info("Connected to MySQL database")

    # ---------------- CUSTOMERS CLEANING ----------------
    customers = pd.read_csv("../data/customers_raw.csv")
    raw_count = len(customers)

    customers = customers.drop_duplicates()
    customers = customers[customers["email"].notna()]

    customers["phone"] = customers["phone"].apply(normalize_phone)
    customers["registration_date"] = customers["registration_date"].apply(parse_date)

    logging.info(f"Customers cleaned count: {len(customers)}")

    # ---------------- LOAD CUSTOMERS ----------------
    insert_query = """
        INSERT INTO customers
        (first_name, last_name, email, phone, city, registration_date)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    inserted = 0

    for _, row in customers.iterrows():
        try:
            cursor.execute(insert_query, (
                row["first_name"],
                row["last_name"],
                row["email"],
                row["phone"],
                row["city"],
                row["registration_date"]
            ))
            inserted += 1
        except mysql.connector.Error as e:
            logging.warning(f"Skipping record due to error: {e}")

    conn.commit()

    logging.info(f"Customers successfully loaded: {inserted}")

    cursor.close()
    conn.close()

    logging.info("Customer ETL completed successfully")

if __name__ == "__main__":
    main()
