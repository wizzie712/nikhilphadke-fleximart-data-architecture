# NoSQL Database Analysis – FlexiMart

## Section A: Limitations of RDBMS (Relational Databases)

Relational databases like MySQL are designed around fixed schemas, which makes them less suitable for highly diverse and evolving product catalogs. In an e-commerce platform like FlexiMart, different product categories have different attributes. For example, laptops require attributes such as RAM, processor, and storage, while shoes need size, color, and material. Representing all these variations in a relational schema requires multiple nullable columns or additional tables, increasing complexity and reducing performance.

Frequent schema changes are another challenge. Adding new product types often requires altering tables, updating constraints, and modifying application logic, which can cause downtime and maintenance overhead. Additionally, storing customer reviews in relational databases usually involves separate tables and joins, making queries slower and harder to manage. Nested data such as reviews and ratings is not handled efficiently in traditional RDBMS systems, especially at scale.


## Section B: Benefits of MongoDB (NoSQL)

MongoDB addresses these challenges by using a flexible, document-based schema. Each product can have its own structure, allowing different attributes to exist for different product categories without enforcing a rigid schema. This makes MongoDB ideal for handling diverse and evolving product catalogs where attributes vary widely across categories.

MongoDB also supports embedded documents, allowing customer reviews to be stored directly within product documents. This reduces the need for complex joins and improves read performance. Horizontal scalability is another key benefit, as MongoDB can scale across multiple servers using sharding. This makes it well-suited for high-volume e-commerce platforms like FlexiMart, where product data and user interactions grow rapidly over time.


## Section C: Trade-offs of Using MongoDB

Despite its flexibility, MongoDB has some disadvantages compared to MySQL. One major trade-off is weaker support for complex transactions and joins, which are critical for financial and order-based systems. Ensuring strong data consistency across multiple collections can be more challenging in MongoDB.

Another disadvantage is increased responsibility on the application layer. Since MongoDB does not enforce strict schemas, developers must handle data validation and consistency themselves. This can lead to inconsistent data if not managed carefully, making MongoDB less suitable for highly structured transactional workloads.
