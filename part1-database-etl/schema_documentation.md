# FlexiMart Database Schema Documentation

## Overview
The FlexiMart transactional database is designed using a relational model to support customer orders, products, and sales transactions. The schema enforces data integrity through primary and foreign key relationships.

---

## Tables Description

### 1. customers
Stores customer master data.

Columns:
- customer_id (PK): Surrogate key identifying each customer
- first_name: Customer first name
- last_name: Customer last name
- email: Unique email address (mandatory)
- phone: Standardized contact number
- city: Customer city
- registration_date: Date of registration

---

### 2. products
Stores product catalog information.

Columns:
- product_id (PK): Surrogate key identifying each product
- product_name: Name of the product
- category: Product category (Electronics, Fashion, Groceries)
- price: Product price
- stock_quantity: Available stock (default 0)

---

### 3. orders
Stores order-level transaction details.

Columns:
- order_id (PK): Surrogate key identifying each order
- customer_id (FK): References customers(customer_id)
- order_date: Date of order
- total_amount: Total order value
- status: Order status (Completed, Cancelled, Pending)

---

### 4. order_items
Stores line-item details for each order.

Columns:
- order_item_id (PK): Surrogate key
- order_id (FK): References orders(order_id)
- product_id (FK): References products(product_id)
- quantity: Quantity purchased
- unit_price: Price per unit
- subtotal: Line item total (quantity × unit_price)

---

## Relationships
- One customer can place many orders
- One order can contain multiple order items
- Each order item refers to one product
- Referential integrity is enforced using foreign keys
