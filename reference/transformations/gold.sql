-- Gold: one table per consumer, at the grain that consumer works at.

CREATE OR REFRESH MATERIALIZED VIEW gold_franchise_daily
COMMENT 'Revenue and footfall per franchise and day'
AS
SELECT
  franchiseID,
  franchise_name,
  franchise_city,
  DATE(dateTime) AS sale_date,
  COUNT(*) AS transaction_count,
  COUNT(DISTINCT customerID) AS customer_count,
  ROUND(SUM(totalPrice), 2) AS revenue,
  ROUND(AVG(totalPrice), 2) AS average_transaction_value
FROM silver_transactions
GROUP BY franchiseID, franchise_name, franchise_city, DATE(dateTime);

CREATE OR REFRESH MATERIALIZED VIEW gold_product_daily
COMMENT 'Units sold and revenue per product and day'
AS
SELECT
  DATE(dateTime) AS sale_date,
  product,
  SUM(quantity) AS units_sold,
  ROUND(SUM(totalPrice), 2) AS revenue
FROM silver_transactions
GROUP BY DATE(dateTime), product;
