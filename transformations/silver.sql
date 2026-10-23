-- Silver: the transactions with a defined set of columns and types that the
-- gold layer and reports build on.

CREATE OR REFRESH MATERIALIZED VIEW silver_transactions
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
