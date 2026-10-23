-- Silver: the transactions with a defined set of columns and types that the
-- gold layer and reports build on.

CREATE OR REFRESH MATERIALIZED VIEW silver_transactions (
  CONSTRAINT typical_basket EXPECT (quantity BETWEEN 1 AND 20)
)
COMMENT 'Transactions, typed and enriched with the franchise they were sold at'
AS
SELECT
  t.transactionID,
  t.customerID,
  t.franchiseID,
  t.dateTime,
  t.product,
  t.quantity,
  -- an example of setting a type in silver; the source prices are whole numbers
  CAST(t.unitPrice AS DECIMAL(12, 2)) AS unitPrice,
  CAST(t.totalPrice AS DECIMAL(12, 2)) AS totalPrice,
  t.paymentMethod,
  f.name AS franchise_name,
  f.city AS franchise_city
FROM bronze_transactions t
-- LEFT JOIN keeps a sale even if its franchise is missing from the master
LEFT JOIN bronze_franchises f
  ON t.franchiseID = f.franchiseID;

CREATE OR REFRESH MATERIALIZED VIEW silver_customer_orders
COMMENT 'Transactions with the customer who placed them'
AS
SELECT
  t.transactionID,
  t.customerID,
  c.first_name,
  c.last_name,
  c.city AS customer_city,
  c.country AS customer_country,
  t.franchiseID,
  t.dateTime,
  t.product,
  t.quantity,
  t.totalPrice
FROM silver_transactions t
-- LEFT JOIN keeps a sale even if its customer is missing from the master
LEFT JOIN bronze_customers c
  ON t.customerID = c.customerID;

CREATE OR REFRESH STREAMING TABLE silver_customers
COMMENT 'Customers as they are now, kept in line with the change feed';

CREATE FLOW silver_customers_cdc AS AUTO CDC INTO silver_customers
FROM STREAM(customer_changes)
KEYS (customerID)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, sequence_num, arrival_order)
STORED AS SCD TYPE 1;

CREATE OR REFRESH STREAMING TABLE silver_customers_history
COMMENT 'Every version of each customer from the change feed';

CREATE FLOW silver_customers_history_cdc AS AUTO CDC INTO silver_customers_history
FROM STREAM(customer_changes)
KEYS (customerID)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, sequence_num, arrival_order)
STORED AS SCD TYPE 2;
