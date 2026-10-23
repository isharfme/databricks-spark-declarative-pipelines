# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold business outputs
# MAGIC
# MAGIC By the end of this notebook the pipeline will publish two gold tables that
# MAGIC show which franchises and products drive revenue: revenue per franchise and
# MAGIC day, and demand per product and day. The finished file is
# MAGIC `reference/transformations/gold.sql` if you get stuck.

# COMMAND ----------

import re

SCHEMA = re.sub(r"\W", "_", spark.sql("SELECT current_user()").first()[0].split("@")[0])  # your own schema, named after your user
spark.catalog.setCurrentCatalog("dbacademy")
spark.catalog.setCurrentDatabase(SCHEMA)
print(f"Your schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build one table per consumer
# MAGIC
# MAGIC Silver holds the trusted transactions; gold shapes them for the people who
# MAGIC use them. Each gold table answers one team's question, at the grain that team
# MAGIC works at:
# MAGIC
# MAGIC | Table | For | Grain |
# MAGIC | --- | --- | --- |
# MAGIC | `gold_franchise_daily` | Franchise operators | one row per franchise and day |
# MAGIC | `gold_product_daily` | Product teams | one row per product and day |
# MAGIC
# MAGIC Both read `silver_transactions`, so the `typical_basket` rule from notebook
# MAGIC 03 is measured once, in silver, and not repeated in gold. Each keeps the
# MAGIC dimensions its readers will filter by (franchise, city, product, day); it's
# MAGIC easy to aggregate further in a query, but impossible to recover a dimension
# MAGIC the table has already summed away.
# MAGIC
# MAGIC Gold tables are materialized views for the reasons from notebook 02. An
# MAGIC aggregate has to reflect the whole source, which a streaming table, appending
# MAGIC only new rows, wouldn't do. And gold is what every report and dashboard
# MAGIC reads, so the aggregation should run once per update rather than on every
# MAGIC read, as it would in a view.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.1 — Two gold tables
# MAGIC
# MAGIC The gold layer gets its own file, `gold.sql`. Create it in the
# MAGIC `transformations` folder: in the pipeline editor, click the plus icon next to
# MAGIC the root folder, select **Transformation**, choose **SQL**, and make sure the
# MAGIC file lands in `transformations`; or create it from the workspace file
# MAGIC browser. Either way works, as long as it's in that folder.
# MAGIC
# MAGIC **`gold_franchise_daily`.** Paste this statement into `gold.sql`. `DATE()`
# MAGIC cuts the timestamp down to the day, and every column that isn't aggregated is
# MAGIC in the `GROUP BY`.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW gold_franchise_daily
# MAGIC COMMENT 'Revenue and footfall per franchise and day'
# MAGIC AS
# MAGIC SELECT
# MAGIC   franchiseID,
# MAGIC   franchise_name,
# MAGIC   franchise_city,
# MAGIC   DATE(dateTime) AS sale_date,
# MAGIC   COUNT(*) AS transaction_count,
# MAGIC   COUNT(DISTINCT customerID) AS customer_count,
# MAGIC   ROUND(SUM(totalPrice), 2) AS revenue,
# MAGIC   ROUND(AVG(totalPrice), 2) AS average_transaction_value
# MAGIC FROM silver_transactions
# MAGIC GROUP BY franchiseID, franchise_name, franchise_city, DATE(dateTime);
# MAGIC ```
# MAGIC
# MAGIC **`gold_product_daily`.** Let Genie Code write this one. Open Genie Code in
# MAGIC the pipeline editor and ask it for the table. Say precisely what you want:
# MAGIC the grain, the column names, and where the statement goes. A vague prompt
# MAGIC gets you a plausible table, not necessarily the one you need. If you'd like
# MAGIC a starting point, this prompt describes it fully:
# MAGIC
# MAGIC > In `transformations/gold.sql`, after `gold_franchise_daily`, add a
# MAGIC > materialized view `gold_product_daily` from `silver_transactions`, with one
# MAGIC > row per day and product. Columns: `sale_date` (the date of `dateTime`),
# MAGIC > `product`, `units_sold` (the sum of `quantity`) and `revenue` (the sum of
# MAGIC > `totalPrice`, rounded to two decimals). Add a `COMMENT` that describes the
# MAGIC > table.
# MAGIC
# MAGIC Read what it generates before you accept it, as you would a colleague's
# MAGIC code: check that it's a `CREATE OR REFRESH MATERIALIZED VIEW`, that it reads
# MAGIC `silver_transactions` by bare name, and that the `GROUP BY` matches the
# MAGIC grain. The check below tests the result either way.
# MAGIC
# MAGIC Run the pipeline. Both tables appear in the graph, hanging off
# MAGIC `silver_transactions`. Then run the check below.

# COMMAND ----------

# Check — run after the pipeline update with both gold tables has finished
from pyspark.sql import functions as F

silver = spark.table("silver_transactions")
franchise = spark.table("gold_franchise_daily")
product = spark.table("gold_product_daily")

assert set(product.columns) == {"sale_date", "product", "units_sold", "revenue"}, f"unexpected columns: {product.columns}"
assert product.groupBy("sale_date", "product").count().filter("count > 1").count() == 0, \
    "more than one row per day and product; check the GROUP BY"

# a gold table must add up to its source: aggregation regroups rows, it never adds or loses them
silver_revenue = silver.agg(F.sum("totalPrice")).first()[0]
assert franchise.agg(F.sum("revenue")).first()[0] == silver_revenue, "gold_franchise_daily revenue doesn't add up to silver"
assert product.agg(F.sum("revenue")).first()[0] == silver_revenue, "gold_product_daily revenue doesn't add up to silver"
assert product.agg(F.sum("units_sold")).first()[0] == silver.agg(F.sum("quantity")).first()[0], \
    "units_sold doesn't add up to silver; is it the sum of quantity?"
print("Check passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query gold
# MAGIC
# MAGIC Which franchises drive revenue? The best days across all franchises:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold_franchise_daily
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC And which products? Total units and revenue per product, from the daily
# MAGIC table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product, SUM(units_sold) AS units_sold, SUM(revenue) AS revenue
# MAGIC FROM gold_product_daily
# MAGIC GROUP BY product
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Neither query needs a join or knows anything about bronze; the pipeline did
# MAGIC that work once, and every consumer reads the result. We stop at queries
# MAGIC here; a dashboard on these tables is a natural next step, and needs nothing
# MAGIC from the pipeline beyond what it already publishes.
# MAGIC
# MAGIC Next: open `05-dashboard` (optional) or go straight to `06-cdc`, both in
# MAGIC the file browser on the left.
