# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Data quality
# MAGIC
# MAGIC By the end of this notebook `silver_transactions` will carry a data quality
# MAGIC rule, you'll have seen the pipeline count the rows that break it, and you'll
# MAGIC know what each kind of rule does to the data it publishes. The finished file
# MAGIC is `reference/transformations/silver.sql` if you get stuck.

# COMMAND ----------

import re

SCHEMA = re.sub(r"\W", "_", spark.sql("SELECT current_user()").first()[0].split("@")[0])  # your own schema, named after your user
spark.catalog.setCurrentCatalog("dbacademy")
spark.catalog.setCurrentDatabase(SCHEMA)
print(f"Your schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check data quality with expectations
# MAGIC
# MAGIC Data quality rules belong in silver. Bronze accepts everything the source
# MAGIC sends, so nothing is lost; silver is the layer others rely on, so the rules
# MAGIC that say what a valid transaction is live there, once, and every table built
# MAGIC on silver inherits them.
# MAGIC
# MAGIC Outside a pipeline you have two ways to enforce such a rule. A `WHERE` clause
# MAGIC removes the rows that break it, and nothing records how many it removed. A
# MAGIC Delta `CHECK` constraint on a table makes the whole write fail as soon as one
# MAGIC row breaks it; there's no way to keep or drop just the offending rows, and no
# MAGIC count. To know how many rows break a rule, you'd write and schedule a query
# MAGIC of your own.
# MAGIC
# MAGIC SDP has **expectations** for this. An expectation is a rule on the dataset
# MAGIC itself: a `CONSTRAINT` with a SQL condition that every row must meet, in
# MAGIC brackets right after the dataset's name. On every update the pipeline counts
# MAGIC the rows that pass and fail each expectation, and you choose what happens to
# MAGIC a row that fails:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW silver_transactions (
# MAGIC   CONSTRAINT <rule_name> EXPECT (<condition>) [ON VIOLATION DROP ROW | ON VIOLATION FAIL UPDATE]
# MAGIC )
# MAGIC COMMENT '...'
# MAGIC AS SELECT ...
# MAGIC ```
# MAGIC
# MAGIC | Clause | On violation | Use it when |
# MAGIC | --- | --- | --- |
# MAGIC | none (warn) | Keep the row, count the violation | You want to know, but the row is still usable |
# MAGIC | `ON VIOLATION DROP ROW` | Leave the row out, count it | A bad row would corrupt everything downstream |
# MAGIC | `ON VIOLATION FAIL UPDATE` | Stop the update | A violation means something upstream is broken and nobody should consume the result |
# MAGIC
# MAGIC The condition is evaluated against the dataset's *output* columns, so a rule
# MAGIC on `silver_transactions` can use `franchise_name`, which only exists in
# MAGIC silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.1 — Flag unusual baskets
# MAGIC
# MAGIC The Bakehouse sample data is clean, so for this lab we use an artificial
# MAGIC rule that some sales do break: a sale is for between 1 and 20 items. We call
# MAGIC it `typical_basket` and make it a warn rule, with no `ON VIOLATION` clause,
# MAGIC so the larger sales stay in silver and are only counted.
# MAGIC
# MAGIC 1. Open `transformations/silver.sql` in the pipeline editor.
# MAGIC 2. Directly after `CREATE OR REFRESH MATERIALIZED VIEW silver_transactions`
# MAGIC    and before `COMMENT`, add the constraint in brackets. The condition is
# MAGIC    plain SQL, as in a `WHERE` clause; `X BETWEEN a AND b` includes both ends.
# MAGIC    ```sql
# MAGIC    CREATE OR REFRESH MATERIALIZED VIEW silver_transactions (
# MAGIC      CONSTRAINT typical_basket EXPECT (quantity BETWEEN 1 AND 20)
# MAGIC    )
# MAGIC    COMMENT 'Transactions, typed and enriched with the franchise they were sold at'
# MAGIC    AS
# MAGIC    ...
# MAGIC    ```
# MAGIC 3. Click **Run pipeline**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## See the rule work
# MAGIC
# MAGIC The pipeline records the result of every expectation on every update, and
# MAGIC there are two places to read it.
# MAGIC
# MAGIC **In the pipeline editor.** Select `silver_transactions` in the graph, or in
# MAGIC the list of tables below it, and open its data quality section. It lists
# MAGIC `typical_basket` with the number of records that passed and failed in the
# MAGIC selected update. Because it's a warn rule, `silver_transactions` still has
# MAGIC every transaction that `bronze_transactions` has.
# MAGIC
# MAGIC **In the event log.** Everything the editor shows comes from the pipeline's
# MAGIC event log, which you can query like a table. That's where an alert or a
# MAGIC quality dashboard would get its numbers. You read it with `event_log()`, a
# MAGIC table-valued function that takes any table the pipeline publishes. The
# MAGIC expectation results are JSON inside its `details` column, which SQL unpacks
# MAGIC with the `:` path syntax and `from_json`. This query lists them per update:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT timestamp, e.dataset, e.name AS expectation, e.passed_records, e.failed_records
# MAGIC FROM (
# MAGIC   SELECT timestamp, explode(from_json(
# MAGIC            details:flow_progress:data_quality:expectations,
# MAGIC            'array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>'
# MAGIC          )) AS e
# MAGIC   FROM event_log(TABLE(silver_transactions))
# MAGIC   WHERE event_type = 'flow_progress'
# MAGIC )
# MAGIC ORDER BY timestamp DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Only the owner of the pipeline's tables can query the event log, which here
# MAGIC is you. Then run the check below.

# COMMAND ----------

# Check — run after the pipeline update with typical_basket has finished
silver = spark.table("silver_transactions")
flagged = silver.filter("quantity NOT BETWEEN 1 AND 20").count()

assert silver.count() == spark.table("bronze_transactions").count(), \
    "silver has fewer rows than bronze; typical_basket should warn, not drop"

failed = spark.sql("""
    SELECT MAX(e.failed_records) AS failed
    FROM (
      SELECT explode(from_json(
               details:flow_progress:data_quality:expectations,
               'array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>'
             )) AS e
      FROM event_log(TABLE(silver_transactions))
      WHERE event_type = 'flow_progress'
    )
    WHERE e.name = 'typical_basket'
""").first().failed
assert failed is not None, "typical_basket isn't in the event log yet; has the update with the constraint finished?"
assert failed == flagged, f"the event log reports {failed} failed records, silver has {flagged} sales outside 1-20"
print(f"Check passed: typical_basket flagged {failed} transactions.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decide what should stop publication
# MAGIC
# MAGIC Which action a rule gets is a business decision, and it helps to be exact
# MAGIC about what each one does to the table people read. Suppose an update
# MAGIC processes 200,000 transactions and record 100,000 breaks the rule:
# MAGIC
# MAGIC - **Warn.** All 200,000 rows are published, record 100,000 included, and
# MAGIC   the failed count goes up by one. Readers see everything; the count tells
# MAGIC   you how much of it is suspect.
# MAGIC - **`DROP ROW`.** 199,999 rows are published. Record 100,000 isn't written to
# MAGIC   the table, and it isn't kept anywhere else by the pipeline either; only the
# MAGIC   count remains. It's still in bronze, which is one reason bronze keeps every
# MAGIC   source row.
# MAGIC - **`FAIL UPDATE`.** Nothing from this update is published. The update stops
# MAGIC   at record 100,000 and the write to the table is rolled back, so the table
# MAGIC   doesn't hold records 1 to 99,999 either: readers keep the version from the
# MAGIC   last successful update, never a partial one. The update shows as failed,
# MAGIC   with an error naming the expectation, and stays that way until someone
# MAGIC   fixes the data or the rule and runs it again.
# MAGIC
# MAGIC So `FAIL UPDATE` is for rules whose violation means the whole load is
# MAGIC suspect, `DROP ROW` for rows that are unusable while the rest of the load is
# MAGIC fine, and warn for rows that are usable but worth watching.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Side note: quarantine the rows a rule drops
# MAGIC
# MAGIC A `DROP ROW` rule removes a row from the target, and the pipeline keeps only
# MAGIC the count. When someone needs to see and fix those rows, a common pattern
# MAGIC (Databricks documents it as *quarantining invalid records*) is a second
# MAGIC dataset that selects them from bronze with the opposite condition. Here is
# MAGIC that query for `typical_basket`; run it to see the rows it would hold. (With
# MAGIC our warn rule they're in silver too, but this is what a `DROP ROW` version
# MAGIC would set aside.)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze_transactions
# MAGIC WHERE NOT (quantity BETWEEN 1 AND 20)

# COMMAND ----------

# MAGIC %md
# MAGIC To make the quarantine part of the pipeline, you'd declare this query as a
# MAGIC dataset in `silver.sql`. A view
# MAGIC (`CREATE VIEW silver_transactions_quarantine AS ...`) is usually enough: the
# MAGIC people who fix the data read it now and then, and a view stores nothing and
# MAGIC always shows the current rows. A materialized view is worth it only if many
# MAGIC people or a dashboard query the quarantine often. Either way, you never move
# MAGIC rows out of it by hand. Once a row is corrected at the source and the
# MAGIC pipeline has refreshed bronze, the row passes silver's rule and drops out of
# MAGIC the quarantine. We don't add it to the pipeline in this workshop.
# MAGIC
# MAGIC Next: open `04-gold` from the file browser on the left.
