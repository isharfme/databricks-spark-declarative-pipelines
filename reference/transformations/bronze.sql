-- Bronze: each Bakehouse source table as the pipeline sees it. The shared tables
-- are read-only, so the pipeline keeps its own copy and builds everything else
-- from that.

CREATE OR REFRESH MATERIALIZED VIEW bronze_transactions
COMMENT 'Sales transactions, as read from samples.bakehouse'
AS SELECT * FROM samples.bakehouse.sales_transactions;

CREATE OR REFRESH MATERIALIZED VIEW bronze_franchises
COMMENT 'Franchise master, as read from samples.bakehouse'
AS SELECT * FROM samples.bakehouse.sales_franchises;

CREATE OR REFRESH MATERIALIZED VIEW bronze_customers
COMMENT 'Customer master, as read from samples.bakehouse'
AS SELECT * FROM samples.bakehouse.sales_customers;
