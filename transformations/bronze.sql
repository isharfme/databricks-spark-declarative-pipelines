-- Bronze: each Bakehouse source table as the pipeline sees it. The shared tables
-- are read-only, so the pipeline keeps its own copy and builds everything else
-- from that.

CREATE OR REFRESH MATERIALIZED VIEW bronze_transactions
COMMENT 'Sales transactions, as read from samples.bakehouse'
AS SELECT * FROM samples.bakehouse.sales_transactions;

-- Exercise 2.1: declare bronze_franchises and bronze_customers below, the same
-- way, from samples.bakehouse.sales_franchises and samples.bakehouse.sales_customers.
