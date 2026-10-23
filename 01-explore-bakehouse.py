# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Explore Bakehouse
# MAGIC
# MAGIC By the end of this notebook you'll know what a data pipeline is, what Spark
# MAGIC Declarative Pipelines does for you, and how a pipeline and a job work
# MAGIC together. You'll also have looked at the three Bakehouse tables your
# MAGIC pipeline reads and the keys that connect them. There is nothing to build
# MAGIC yet; the pipeline starts in notebook 02.
# MAGIC
# MAGIC Pick **Serverless** in the compute selector at the top right before you run
# MAGIC anything.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Know what a pipeline is
# MAGIC
# MAGIC A **data pipeline** takes data from where it arrives, cleans and combines it,
# MAGIC and publishes the result for people and applications to use. It does that in
# MAGIC steps, and each step reads what earlier steps produced. Drawn out, the steps
# MAGIC form a graph in which data flows one way and never loops back — a directed
# MAGIC acyclic graph, or DAG.
# MAGIC
# MAGIC This workshop arranges the steps in three layers, a common layout known as the
# MAGIC medallion architecture:
# MAGIC
# MAGIC - **Bronze** holds the source data as it arrived, so the original rows are
# MAGIC   always there to rebuild from.
# MAGIC - **Silver** holds the data cleaned, typed and combined. It's the layer other
# MAGIC   tables and reports build on.
# MAGIC - **Gold** holds the data aggregated for one particular use, such as revenue
# MAGIC   per franchise and day.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Know what you'll build it with
# MAGIC
# MAGIC You'll build the pipeline with **Spark Declarative Pipelines on Lakeflow**
# MAGIC (SDP). Apache Spark™ Declarative Pipelines is a declarative framework for
# MAGIC building batch and streaming data pipelines in SQL and Python. Lakeflow
# MAGIC pipelines extend and are interoperable with it, while running on the
# MAGIC performance-optimized Databricks Runtime. Typical uses are ingesting data from
# MAGIC cloud storage and message buses, and incremental batch and streaming
# MAGIC transformations.
# MAGIC
# MAGIC *Declarative* means you describe each step by the result it should produce,
# MAGIC as a query, rather than writing the commands that produce it and the order
# MAGIC they run in. SDP reads all the queries, works out which step reads which, and
# MAGIC builds the DAG itself. One run of a pipeline is called an **update**: it
# MAGIC brings every step up to date in the order the graph dictates, processes only
# MAGIC new data where it can, and records what happened. Notebook 02 puts the two
# MAGIC ways of building a pipeline side by side.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Know what a job adds
# MAGIC
# MAGIC A pipeline knows how to build its data, but not when to run. That's the role
# MAGIC of a **job** (Lakeflow Jobs). A job runs one or more *tasks* — a notebook, a
# MAGIC SQL query, a Python script, or a pipeline — on a schedule or when something
# MAGIC happens, such as new files arriving, in the order you configure, with retries
# MAGIC and notifications.
# MAGIC
# MAGIC So in production a job typically runs the pipeline as one of its tasks, for
# MAGIC example every night, perhaps followed by a task that sends a report. In this
# MAGIC workshop you start the pipeline by hand, so there's no job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Meet the Bakehouse data
# MAGIC
# MAGIC Bakehouse is a sample dataset of a fictional chain of cookie franchises. Its
# MAGIC sales system records every transaction, with the franchise it happened at and
# MAGIC the customer who paid, and keeps master data about the franchises and the
# MAGIC customers. Your pipeline reads three of its tables:
# MAGIC
# MAGIC | Table | One row per |
# MAGIC | --- | --- |
# MAGIC | `sales_transactions` | sale |
# MAGIC | `sales_franchises` | franchise |
# MAGIC | `sales_customers` | customer |
# MAGIC
# MAGIC By the end of the workshop the pipeline turns them into this graph, where each
# MAGIC arrow is a dataset reading another:
# MAGIC
# MAGIC ```text
# MAGIC sales_transactions ──> bronze_transactions ──┐
# MAGIC                                              ├─> silver_transactions ──┬─> gold_franchise_daily
# MAGIC sales_franchises ────> bronze_franchises ────┘                         ├─> gold_product_daily
# MAGIC                                                                        └──┐
# MAGIC sales_customers ─────> bronze_customers ──────────────────────────────────┴─> silver_customer_orders
# MAGIC
# MAGIC customer_changes ──┬─> silver_customers
# MAGIC                    └─> silver_customers_history
# MAGIC ```
# MAGIC
# MAGIC `customer_changes` is a small feed of customer changes that you'll create
# MAGIC yourself, because the Bakehouse tables never change.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find the source tables
# MAGIC
# MAGIC In this workshop `samples.bakehouse` plays the role of the landing zone: the
# MAGIC data as it arrives from Bakehouse's sales system, which you read but don't
# MAGIC own. In a real project an ingestion tool would land it there, for example
# MAGIC Auto Loader picking up exported files, or Lakeflow Connect replicating the
# MAGIC sales database. Here the tables are already in place.
# MAGIC
# MAGIC They live in the `samples` catalog, which the workspace receives as a share:
# MAGIC in Catalog Explorer it's listed under *Shares received*. A shared catalog is
# MAGIC read-only, so everyone in the lab reads the same source and nobody can change
# MAGIC it for the others, just as you can't change a source system.
# MAGIC
# MAGIC List what's there:

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN samples.bakehouse

# COMMAND ----------

# MAGIC %md
# MAGIC We use three of them: `sales_transactions`, `sales_franchises` and
# MAGIC `sales_customers`. The others (`sales_suppliers`, `media_customer_reviews`
# MAGIC and `media_gold_reviews_chunked`) aren't used in this workshop.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the tables
# MAGIC
# MAGIC Transactions first. Each row is one sale: who bought (`customerID`), where
# MAGIC (`franchiseID`), when (`dateTime`), what (`product`, `quantity`), for how
# MAGIC much (`unitPrice`, `totalPrice`) and how it was paid (`paymentMethod`,
# MAGIC `cardNumber`).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM samples.bakehouse.sales_transactions LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC The franchise master gives each `franchiseID` a `name`, a location (`city`,
# MAGIC `district`, `country`) and a `size`; the customer master gives each
# MAGIC `customerID` a name, contact details and an address.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM samples.bakehouse.sales_franchises LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM samples.bakehouse.sales_customers LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC Column names and types matter more than the values here: the pipeline's
# MAGIC queries refer to them by name. `DESCRIBE` shows them for one table:

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE samples.bakehouse.sales_transactions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Name the keys
# MAGIC
# MAGIC Before you build anything, check that you can find these in the output
# MAGIC above. They're what every join in the pipeline is built on:
# MAGIC
# MAGIC | What | Column | Table |
# MAGIC | --- | --- | --- |
# MAGIC | Transaction key | `transactionID` | `sales_transactions` |
# MAGIC | Customer key | `customerID` | `sales_transactions` → `sales_customers` |
# MAGIC | Franchise key | `franchiseID` | `sales_transactions` → `sales_franchises` |
# MAGIC | Timestamp | `dateTime` | `sales_transactions` |
# MAGIC | Revenue | `quantity`, `unitPrice`, `totalPrice` | `sales_transactions` |
# MAGIC
# MAGIC Next: open `02-declare-and-run` from the file browser on the left.
