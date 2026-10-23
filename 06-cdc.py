# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Track data changes
# MAGIC
# MAGIC By the end of this notebook your pipeline will keep two customer tables in
# MAGIC line with a feed of customer changes, and you'll have seen how little of the
# MAGIC change processing you write yourself. The rest of the pipeline stays as it
# MAGIC is. The finished statements are at the end of
# MAGIC `reference/transformations/silver.sql` if you get stuck.
# MAGIC
# MAGIC Many source systems don't send a full copy of their data on every load. They
# MAGIC send what changed since the last one: inserts, updates and deletes, a pattern
# MAGIC known as change data capture (CDC). Applying such a feed correctly takes more
# MAGIC than copying rows. The changes have to be applied in the order they happened,
# MAGIC which isn't always the order they arrive in; deletes have to remove records;
# MAGIC and if you want history, every change has to close the old version of a
# MAGIC record and add the new one.
# MAGIC
# MAGIC SDP handles all of that with one kind of flow, `AUTO CDC`. You'll apply the
# MAGIC same feed twice: to `silver_customers`, which keeps each customer's latest
# MAGIC values (a *slowly changing dimension* of type 1, or SCD type 1), and to
# MAGIC `silver_customers_history`, which keeps every version (SCD type 2).

# COMMAND ----------

import re

SCHEMA = re.sub(r"\W", "_", spark.sql("SELECT current_user()").first()[0].split("@")[0])  # your own schema, named after your user
spark.catalog.setCurrentCatalog("dbacademy")
spark.catalog.setCurrentDatabase(SCHEMA)
print(f"Your schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up the change feed
# MAGIC
# MAGIC The Bakehouse tables never change, so the cell below plays the source system:
# MAGIC it writes a small change feed, `customer_changes`, into your schema. The feed
# MAGIC covers two customers, A and B (the first two by `customerID`), and each row
# MAGIC records one change to one of them:
# MAGIC
# MAGIC | `arrival_order` | Customer | `operation` | `sequence_num` | The change |
# MAGIC | ---: | --- | --- | ---: | --- |
# MAGIC | 1 | A | `INSERT` | 1 | A, as in `sales_customers` |
# MAGIC | 2 | B | `INSERT` | 1 | B, as in `sales_customers` |
# MAGIC | 3 | A | `UPDATE` | 3 | A moves to a new city |
# MAGIC | 4 | B | `DELETE` | 2 | B is deleted |
# MAGIC | 5 | A | `UPDATE` | 2 | A moves to another city, a change that happened *before* row 3 |
# MAGIC
# MAGIC Besides the customer's name, city and country, every row says what happened
# MAGIC (`operation`), when it happened in the source system (`sequence_num`; a real
# MAGIC feed would use a commit timestamp or a log position), and when it reached the
# MAGIC feed (`arrival_order`). Only `DELETE` is special; an `INSERT` or `UPDATE` is
# MAGIC applied as an upsert, which inserts the customer or updates them if they're
# MAGIC already there.
# MAGIC
# MAGIC The feed is a regular table outside the pipeline, which the pipeline reads as
# MAGIC a source. It must only ever grow, because the pipeline reads it as a stream of
# MAGIC new rows, and a stream breaks if the table behind it is replaced. So the cell
# MAGIC creates it only if it doesn't exist yet; running it again changes nothing.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customer_changes AS
# MAGIC WITH customers AS (
# MAGIC   SELECT customerID, first_name, last_name, city, country,
# MAGIC          ROW_NUMBER() OVER (ORDER BY customerID) AS position
# MAGIC   FROM samples.bakehouse.sales_customers
# MAGIC ),
# MAGIC a AS (SELECT * FROM customers WHERE position = 1),
# MAGIC b AS (SELECT * FROM customers WHERE position = 2),
# MAGIC -- two other cities for A to move to, taken from other customers so they're real
# MAGIC moves AS (
# MAGIC   SELECT city, country, ROW_NUMBER() OVER (ORDER BY city) AS position
# MAGIC   FROM (SELECT DISTINCT city, country FROM samples.bakehouse.sales_customers)
# MAGIC   WHERE city <> (SELECT city FROM a)
# MAGIC )
# MAGIC SELECT 1 AS arrival_order, customerID, first_name, last_name, city, country, 'INSERT' AS operation, 1 AS sequence_num FROM a
# MAGIC UNION ALL
# MAGIC SELECT 2, customerID, first_name, last_name, city, country, 'INSERT', 1 FROM b
# MAGIC UNION ALL
# MAGIC SELECT 3, a.customerID, a.first_name, a.last_name, m.city, m.country, 'UPDATE', 3 FROM a, moves m WHERE m.position = 2
# MAGIC UNION ALL
# MAGIC SELECT 4, customerID, NULL, NULL, NULL, NULL, 'DELETE', 2 FROM b  -- a delete needs only the key
# MAGIC UNION ALL
# MAGIC SELECT 5, a.customerID, a.first_name, a.last_name, m.city, m.country, 'UPDATE', 2 FROM a, moves m WHERE m.position = 1

# COMMAND ----------

# MAGIC %md
# MAGIC Here is the feed, in the order the changes arrived. You should see the five
# MAGIC rows from the table above, with the real IDs, names and cities of A and B:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customer_changes ORDER BY arrival_order

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spot the change that arrives late
# MAGIC
# MAGIC Follow customer A down the result. In `arrival_order` A's changes come 1, 3,
# MAGIC 5, but their `sequence_num` goes 1, 3, 2: the move that happened second
# MAGIC arrived last. Real feeds do this all the time, for example when a delivery is
# MAGIC retried or several extractors run in parallel. Applied in arrival order, the
# MAGIC late change would overwrite the newer one and leave A in a city they've
# MAGIC already left. Whatever applies the feed has to order the changes by
# MAGIC `sequence_num`, not by when they arrived.
# MAGIC
# MAGIC Before you apply the feed, note where A and B are today, as your pipeline's
# MAGIC `bronze_customers` has them. You should see both customers with their
# MAGIC original city and country:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customerID, first_name, last_name, city, country
# MAGIC FROM bronze_customers
# MAGIC WHERE customerID IN (SELECT customerID FROM customer_changes)
# MAGIC ORDER BY customerID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply the changes the imperative way
# MAGIC
# MAGIC Without SDP, you'd apply the feed to `silver_customers` with a `MERGE`, run
# MAGIC by a job. The cell below is a sketch of the SCD type 1 version. Every line is
# MAGIC commented out, so running the cell changes nothing.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE INTO silver_customers AS t
# MAGIC -- USING (
# MAGIC --   SELECT *
# MAGIC --   FROM customer_changes
# MAGIC --   QUALIFY ROW_NUMBER() OVER (PARTITION BY customerID ORDER BY sequence_num DESC) = 1  -- latest change per customer
# MAGIC -- ) AS s
# MAGIC -- ON t.customerID = s.customerID
# MAGIC -- WHEN MATCHED AND s.sequence_num > t.sequence_num AND s.operation = 'DELETE' THEN DELETE
# MAGIC -- WHEN MATCHED AND s.sequence_num > t.sequence_num THEN UPDATE SET *
# MAGIC -- WHEN NOT MATCHED AND s.operation <> 'DELETE' THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC Every rule of change processing is in that statement, and yours to get right:
# MAGIC
# MAGIC - **Order.** The `QUALIFY` keeps only the latest change per customer within
# MAGIC   a run, and the `s.sequence_num > t.sequence_num` conditions stop a late
# MAGIC   change from overwriting a newer one from an earlier run. For that,
# MAGIC   `silver_customers` has to store `sequence_num` too.
# MAGIC - **Deletes.** A separate branch removes deleted customers, and the insert
# MAGIC   branch has to skip them.
# MAGIC - **New changes only.** As written, every run reads the whole feed again. To
# MAGIC   read only what's new, you'd track how far the last run got.
# MAGIC - **Setup.** Someone has to create `silver_customers` before the first run,
# MAGIC   and a job has to run the `MERGE` after the feed is loaded.
# MAGIC
# MAGIC And that's only SCD type 1. For SCD type 2 you'd add a second `MERGE`, or a
# MAGIC `UNION` that feeds each change in twice, to close a customer's current version
# MAGIC and insert the new one.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.1 — Declare the CDC tables
# MAGIC
# MAGIC In SDP the same rules are clauses of an `AUTO CDC` flow, and the pipeline does
# MAGIC the rest:
# MAGIC
# MAGIC | What the `MERGE` does by hand | What you write in SDP |
# MAGIC | --- | --- |
# MAGIC | Keep the latest change per customer, and stop late changes from overwriting newer ones | `SEQUENCE BY sequence_num` |
# MAGIC | Remove deleted customers, and skip them on insert | `APPLY AS DELETE WHEN operation = 'DELETE'` |
# MAGIC | Read only the changes not applied yet | `FROM STREAM(customer_changes)`: the flow reads the feed as a stream, and the pipeline keeps track of how far it got |
# MAGIC | Create the target before the first run | `CREATE OR REFRESH STREAMING TABLE`; the pipeline creates the target |
# MAGIC | A second `MERGE` for history | `STORED AS SCD TYPE 2` instead of `SCD TYPE 1` |
# MAGIC
# MAGIC `KEYS (customerID)` says which column identifies a customer, and
# MAGIC `COLUMNS * EXCEPT (...)` leaves the change columns out of the target, since
# MAGIC they describe the change, not the customer. The target is a streaming table
# MAGIC because `AUTO CDC` writes only into streaming tables, and a feed that only
# MAGIC grows is what streaming tables are for.
# MAGIC
# MAGIC 1. Open `transformations/silver.sql` and paste the statements below after
# MAGIC    `silver_customer_orders`. A customer table is silver: cleaned reference
# MAGIC    data that other tables can join to.
# MAGIC    ```sql
# MAGIC    CREATE OR REFRESH STREAMING TABLE silver_customers
# MAGIC    COMMENT 'Customers as they are now, kept in line with the change feed';
# MAGIC
# MAGIC    CREATE FLOW silver_customers_cdc AS AUTO CDC INTO silver_customers
# MAGIC    FROM STREAM(customer_changes)
# MAGIC    KEYS (customerID)
# MAGIC    APPLY AS DELETE WHEN operation = 'DELETE'
# MAGIC    SEQUENCE BY sequence_num
# MAGIC    COLUMNS * EXCEPT (operation, sequence_num, arrival_order)
# MAGIC    STORED AS SCD TYPE 1;
# MAGIC    ```
# MAGIC 2. Below them, declare `silver_customers_history` the same way, with a flow
# MAGIC    named `silver_customers_history_cdc` that stores it `AS SCD TYPE 2`.
# MAGIC 3. Run the pipeline.
# MAGIC
# MAGIC **What you should see:** in the graph, `customer_changes` feeds
# MAGIC `silver_customers` and `silver_customers_history`, a branch of its own next to
# MAGIC the rest of the pipeline. Nothing else in the graph changes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## See the result
# MAGIC
# MAGIC When the update has finished, look at `silver_customers`. You should see only
# MAGIC customer A, in the city from `sequence_num` 3. That's the out-of-order case
# MAGIC handled: the change with `sequence_num` 2 arrived later, but `SEQUENCE BY`
# MAGIC put it before 3, so it didn't overwrite A's newer city. B isn't there, because
# MAGIC the flow applied the delete.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_customers ORDER BY customerID

# COMMAND ----------

# MAGIC %md
# MAGIC Then `silver_customers_history`. Each row is one version of a customer, and
# MAGIC `__START_AT` and `__END_AT` mark the range of `sequence_num` values over which
# MAGIC it was current. You should see:
# MAGIC
# MAGIC - Three versions of A, in `sequence_num` order: the original city (1 to 2),
# MAGIC   the city of the earlier move (2 to 3), and the city of the later move (from
# MAGIC   3, with `__END_AT` empty because it's still current). The change that arrived
# MAGIC   last sits in the middle of the history, where it belongs.
# MAGIC - One version of B, closed at 2 by the delete. The history keeps B and records
# MAGIC   when B stopped being current.
# MAGIC
# MAGIC You wrote none of that ordering or versioning logic; it came from
# MAGIC `SEQUENCE BY` and `STORED AS SCD TYPE 2`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_customers_history ORDER BY customerID, __START_AT

# COMMAND ----------

# MAGIC %md
# MAGIC In production, the feed would come from the source system: Lakeflow Connect
# MAGIC replicating a database, or Auto Loader picking up the change files a system
# MAGIC exports. The flows stay as they are. Each update applies whatever has arrived
# MAGIC since the last one, in `sequence_num` order, whatever order it arrived in.
# MAGIC
# MAGIC Next: open `07-operate` from the file browser on the left.
