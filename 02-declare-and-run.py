# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Declare the pipeline and run it
# MAGIC
# MAGIC By the end of this notebook you'll have a running pipeline that copies the
# MAGIC three Bakehouse tables into bronze, builds an enriched `silver_transactions`,
# MAGIC and joins customers onto it in `silver_customer_orders`. You'll write part of
# MAGIC the SQL yourself. The finished version of every pipeline file is in
# MAGIC `reference/transformations`, which you can open from the file browser on the
# MAGIC left if you get stuck.
# MAGIC
# MAGIC Most of the work happens in the pipeline editor. This notebook holds the
# MAGIC explanation, the steps, and the cells that check your results.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a table the imperative way
# MAGIC
# MAGIC Take one piece of the pipeline: `silver_transactions`, every sale with the
# MAGIC name and city of the franchise it happened at, built from copies of the
# MAGIC transactions and franchises tables.
# MAGIC
# MAGIC Without SDP, you'd build it imperatively: a notebook with three cells, one
# MAGIC per step, and a job that runs the notebook on a schedule.
# MAGIC
# MAGIC ```sql
# MAGIC -- cell 1: copy the transactions
# MAGIC CREATE OR REPLACE TABLE bronze_transactions AS SELECT * FROM samples.bakehouse.sales_transactions;
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC -- cell 2: copy the franchises
# MAGIC CREATE OR REPLACE TABLE bronze_franchises AS SELECT * FROM samples.bakehouse.sales_franchises;
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC -- cell 3: join them
# MAGIC CREATE OR REPLACE TABLE silver_transactions AS
# MAGIC SELECT t.transactionID, ..., f.name AS franchise_name, f.city AS franchise_city
# MAGIC FROM bronze_transactions t
# MAGIC LEFT JOIN bronze_franchises f ON t.franchiseID = f.franchiseID;
# MAGIC ```
# MAGIC
# MAGIC Each statement says what its table contains, but the pipeline itself (which
# MAGIC step runs when) is defined somewhere else. The order of the steps is the order
# MAGIC of the cells: cell 3 reads what cells 1 and 2 wrote, so it has to come after
# MAGIC them, and nothing checks that it does. When the notebook runs, and what runs
# MAGIC before and after it, is set in the job. Adding a dataset means adding a cell
# MAGIC in the right position, and every run executes every cell in full.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the same table declaratively
# MAGIC
# MAGIC In SDP you define the pipeline as a set of datasets, with one statement each
# MAGIC that says what the dataset should contain:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW bronze_transactions AS SELECT * FROM samples.bakehouse.sales_transactions;
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW bronze_franchises AS SELECT * FROM samples.bakehouse.sales_franchises;
# MAGIC
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW silver_transactions AS
# MAGIC SELECT t.transactionID, ..., f.name AS franchise_name, f.city AS franchise_city
# MAGIC FROM bronze_transactions t
# MAGIC LEFT JOIN bronze_franchises f ON t.franchiseID = f.franchiseID;
# MAGIC ```
# MAGIC
# MAGIC There's no order and nothing that wires the statements together. SDP reads
# MAGIC every statement, sees from the `FROM` and `JOIN` clauses that
# MAGIC `silver_transactions` reads `bronze_transactions` and `bronze_franchises`,
# MAGIC builds the DAG, and runs `silver_transactions` after them. On each update it
# MAGIC brings every dataset in line with its query, working out itself what has
# MAGIC changed since the last update and processing only that where it can. Adding
# MAGIC a dataset means adding a statement to any of the pipeline's files.
# MAGIC `CREATE OR REFRESH` means "create the dataset if it doesn't exist yet,
# MAGIC otherwise bring it up to date".
# MAGIC
# MAGIC The other difference is the keyword: the notebook creates tables, while the
# MAGIC pipeline declares *materialized views*. The next section explains why.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Choose the dataset type
# MAGIC
# MAGIC Each step in the pipeline's DAG is a dataset defined by a query, and SDP
# MAGIC lets you declare three types of dataset:
# MAGIC
# MAGIC | Type | What it stores | Suits |
# MAGIC | --- | --- | --- |
# MAGIC | **Streaming table** (`CREATE OR REFRESH STREAMING TABLE`) | Each source row, processed once as it arrives and appended | Sources that only grow, such as files landing in cloud storage or events on a message bus |
# MAGIC | **Materialized view** (`CREATE OR REFRESH MATERIALIZED VIEW`) | The result of its query over the whole source, kept in line with the source on every update | Joins, aggregations, and sources that are tables rather than feeds |
# MAGIC | **View** (`CREATE VIEW`, or `CREATE TEMPORARY VIEW` to keep it inside the pipeline) | Nothing; its query runs each time it's read | Intermediate steps, and results read rarely enough that storing them isn't worth it |
# MAGIC
# MAGIC SDP doesn't declare regular tables. The tables a pipeline publishes are its
# MAGIC streaming tables and materialized views, which SDP creates and refreshes
# MAGIC itself. A dataset can also be defined in only one pipeline, so two pipelines
# MAGIC can't write the same table. (The exception is a streaming table fed by
# MAGIC several append flows, which this workshop doesn't use.) Regular tables do
# MAGIC appear in a pipeline, as sources: the `samples.bakehouse` tables are regular
# MAGIC tables that the pipeline reads.
# MAGIC
# MAGIC Every dataset in this workshop is a materialized view, and here is why:
# MAGIC
# MAGIC - **Not a streaming table.** A streaming table needs a source that only ever
# MAGIC   appends rows. The Bakehouse tables are snapshots: nothing guarantees that a
# MAGIC   row, once there, never changes. A materialized view reflects whatever the
# MAGIC   source contains at each update. If the source were a feed of new sales, you'd
# MAGIC   make bronze a streaming table and keep the rest as it is.
# MAGIC - **Not a view.** Bronze is the pipeline's own copy of the source, and a view
# MAGIC   stores nothing, so it would only read the source again each time. Silver and
# MAGIC   gold have many readers: the next layer, ad hoc queries, a dashboard. A view
# MAGIC   would rerun its joins and aggregations for each of them, on every read. A
# MAGIC   materialized view runs them once per update, and every reader gets the
# MAGIC   stored result.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC You share this workspace with the other participants, so the pipeline
# MAGIC publishes into a schema of your own in the catalog `dbacademy`, named after
# MAGIC your user. Run the setup cell; you'll need the schema name when you create
# MAGIC the pipeline:

# COMMAND ----------

import re

SCHEMA = re.sub(r"\W", "_", spark.sql("SELECT current_user()").first()[0].split("@")[0])  # your own schema, named after your user
spark.catalog.setCurrentCatalog("dbacademy")
spark.catalog.setCurrentDatabase(SCHEMA)
print(f"Your schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Know how a pipeline project is laid out
# MAGIC
# MAGIC The pipeline's code doesn't live in this notebook. It lives in source files
# MAGIC in the `transformations` folder next to it.
# MAGIC
# MAGIC **Can a notebook be the pipeline?** Not in the pipeline editor you'll use
# MAGIC here: a pipeline's source code is a set of `.sql` and `.py` files. (Pipelines
# MAGIC whose source code was a notebook belong to an older editing experience that
# MAGIC Databricks has deprecated.) The two also run differently. A notebook runs
# MAGIC cell by cell, in the order you click, as the three cells above did. A
# MAGIC pipeline reads all its source files together, builds one DAG from every
# MAGIC statement in them, and runs that DAG. So the pipeline's code is the files,
# MAGIC and a notebook like this one is for the work around it: exploring the data,
# MAGIC explaining the steps, and checking the results.
# MAGIC
# MAGIC **SQL or Python?** A pipeline file can be SQL or Python, and one pipeline can
# MAGIC mix both: a dataset in a Python file can read one defined in a SQL file. The
# MAGIC two offer equivalent functionality for most data processing, so choose the
# MAGIC one you and your team work in most comfortably. SQL reads well when every
# MAGIC dataset is a query (joins, filters, aggregations), as in this pipeline.
# MAGIC Python helps when you'd rather generate the datasets than write each one,
# MAGIC for example one bronze dataset per source table, declared in a loop. This
# MAGIC workshop uses SQL.
# MAGIC
# MAGIC **One file per layer.** We put each layer in its own file (`bronze.sql`,
# MAGIC `silver.sql`, and later `gold.sql`) only to keep each file short and easy to
# MAGIC find. The pipeline doesn't care how the code is split. There's no order
# MAGIC between the files, and a statement in one file can read a dataset defined in
# MAGIC another; SDP works out the DAG from the references in the queries. One file
# MAGIC for the whole pipeline, or one file per dataset, would give the same graph.
# MAGIC
# MAGIC How a file gets into the folder doesn't matter either. The pipeline's source
# MAGIC is the `transformations` folder as a whole, so a file you create there from
# MAGIC the pipeline editor and one you create from the workspace file browser are
# MAGIC both part of the pipeline from its next update. The pipeline editor is the
# MAGIC more convenient place, because it shows the file next to the graph and can
# MAGIC run it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the starting files
# MAGIC
# MAGIC Open the `transformations` folder. It holds two files:
# MAGIC
# MAGIC | File | Layer | Datasets so far |
# MAGIC | --- | --- | --- |
# MAGIC | `bronze.sql` | bronze | `bronze_transactions` |
# MAGIC | `silver.sql` | silver | `silver_transactions` |
# MAGIC
# MAGIC **`bronze.sql`** holds the bronze layer. It starts with one dataset, and
# MAGIC you'll add the other two in Exercise 2.1:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW bronze_transactions
# MAGIC COMMENT 'Sales transactions, as read from samples.bakehouse'
# MAGIC AS SELECT * FROM samples.bakehouse.sales_transactions;
# MAGIC ```
# MAGIC
# MAGIC Bronze takes every column with `SELECT *`, because its job is to mirror the
# MAGIC source as it is: if a rule further up turns out to be wrong, the original
# MAGIC rows are still there to rebuild from.
# MAGIC
# MAGIC **`silver.sql`** holds the silver layer, which so far is
# MAGIC `silver_transactions`. Silver is the layer other tables and reports build
# MAGIC on, so it has a defined schema: a deliberate choice of columns and types that
# MAGIC they can rely on, rather than whatever the source happens to send. Here's the
# MAGIC reasoning behind its query:
# MAGIC
# MAGIC - **It names the columns it keeps.** `cardNumber` is left out: nobody
# MAGIC   analysing revenue needs a card number, and leaving it in bronze keeps it out
# MAGIC   of every table built on silver. Naming the columns also means silver doesn't
# MAGIC   change when the source adds a column, until someone decides it should.
# MAGIC - **It adds the franchise.** A franchise ID means nothing to someone reading
# MAGIC   revenue figures, so each sale gets the franchise's name and city. They're
# MAGIC   called `franchise_name` and `franchise_city` because silver will also hold
# MAGIC   customer details, and a bare `name` or `city` wouldn't say whose. The
# MAGIC   `LEFT JOIN` keeps a sale even when its franchise is missing from the master;
# MAGIC   dropping it would understate revenue.
# MAGIC - **It casts the prices to `DECIMAL(12, 2)`.** The source stores them as
# MAGIC   whole numbers, so this changes no values. It's here only as an example of a
# MAGIC   type change, the kind of transformation silver is the place for: in real
# MAGIC   data you might parse a date stored as text, or fix the precision of an
# MAGIC   amount.
# MAGIC
# MAGIC `silver_transactions` reads `bronze_franchises`, which doesn't exist yet.
# MAGIC That's the first exercise.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.1 — Declare the rest of bronze
# MAGIC
# MAGIC Open `transformations/bronze.sql` and add two statements below
# MAGIC `bronze_transactions`, following the same pattern:
# MAGIC
# MAGIC - `bronze_franchises`, from `samples.bakehouse.sales_franchises`
# MAGIC - `bronze_customers`, from `samples.bakehouse.sales_customers`
# MAGIC
# MAGIC Give each a `COMMENT` that says what it holds. End every statement with a
# MAGIC `;`, because a file can hold several.
# MAGIC
# MAGIC You'll see the result when you run the pipeline in the next step.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create and run the pipeline
# MAGIC
# MAGIC Pipelines are managed under *Jobs & Pipelines* in the sidebar. Open it in a
# MAGIC separate browser tab (for example, by right-clicking it and choosing to open
# MAGIC it in a new tab), so you can switch between this notebook and the pipeline
# MAGIC editor.
# MAGIC
# MAGIC 1. In *Jobs & Pipelines*, click **Create** and select **ETL pipeline**.
# MAGIC    New pipelines use Unity Catalog and serverless compute by default.
# MAGIC 2. Name it after your schema, as printed above, followed by
# MAGIC    `_bakehouse_pipeline` (`<your schema>_bakehouse_pipeline`).
# MAGIC 3. Next to the name, set the default catalog to `dbacademy` and the default
# MAGIC    schema to your own schema, as printed above.
# MAGIC 4. Choose to add existing assets rather than sample code. Set the root
# MAGIC    folder to the lab folder this notebook is in, and the source code path
# MAGIC    to the `transformations` folder inside it. (You can change the paths
# MAGIC    later, under **Settings** → **Source code**.)
# MAGIC 5. Click **Run pipeline**.
# MAGIC
# MAGIC **Why the default catalog and schema matter.** Every dataset the pipeline
# MAGIC defines, and every table it reads by bare name, is resolved to the default
# MAGIC catalog and schema. That's why the SQL files never mention `dbacademy` or
# MAGIC your schema, and why `silver_transactions` can say just
# MAGIC `FROM bronze_transactions`. It also means the code doesn't decide where the
# MAGIC tables end up; the pipeline's settings do. The same files can publish into
# MAGIC another participant's schema, or into a development catalog and a
# MAGIC production catalog, without changing a line of SQL. Only tables outside the
# MAGIC pipeline, such as `samples.bakehouse.sales_transactions`, are named in full.
# MAGIC
# MAGIC The first update takes a few minutes, because serverless compute starts and
# MAGIC every table is built from scratch.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the graph
# MAGIC
# MAGIC While the update runs, look at the graph below the editor. The pipeline drew
# MAGIC it from the two files without being told anything about order:
# MAGIC
# MAGIC - the three bronze datasets from `bronze.sql`, with nothing upstream of them
# MAGIC   inside the pipeline
# MAGIC - `silver_transactions` from `silver.sql`, with edges from
# MAGIC   `bronze_transactions` and `bronze_franchises`, taken from its `FROM` and
# MAGIC   `JOIN` clauses
# MAGIC - `bronze_customers`, which nothing reads yet; Exercise 2.2 connects it
# MAGIC
# MAGIC Data flows left to right, and that's the order the update runs in. Click a
# MAGIC dataset in the graph to see its status, row count and duration for this
# MAGIC update.
# MAGIC
# MAGIC When the update has finished, open Catalog Explorer, then `dbacademy` and
# MAGIC your schema. The four datasets are there, each with the comment you gave it.
# MAGIC Open `silver_transactions`: its columns are the ones silver selected, with the
# MAGIC prices as decimals, and its *Sample data* tab shows the franchise name next
# MAGIC to every sale.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.2 — Join the customers
# MAGIC
# MAGIC A layer can hold more than one dataset. The next one in silver,
# MAGIC `silver_customer_orders`, puts the customer next to every sale, for anyone
# MAGIC who analyses sales by customer rather than by franchise.
# MAGIC
# MAGIC 1. Open `transformations/silver.sql` and paste the statement below after
# MAGIC    `silver_transactions`.
# MAGIC 2. Replace `<insert your code>` with the `FROM` clause: read
# MAGIC    `silver_transactions` as `t`, and `LEFT JOIN` `bronze_customers` as `c` on
# MAGIC    the key the two share.
# MAGIC 3. Run the pipeline.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW silver_customer_orders
# MAGIC COMMENT 'Transactions with the customer who placed them'
# MAGIC AS
# MAGIC SELECT
# MAGIC   t.transactionID,
# MAGIC   t.customerID,
# MAGIC   c.first_name,
# MAGIC   c.last_name,
# MAGIC   c.city AS customer_city,
# MAGIC   c.country AS customer_country,
# MAGIC   t.franchiseID,
# MAGIC   t.dateTime,
# MAGIC   t.product,
# MAGIC   t.quantity,
# MAGIC   t.totalPrice
# MAGIC <insert your code>;
# MAGIC ```
# MAGIC
# MAGIC The join reads from `silver_transactions`, not from bronze, so the customer
# MAGIC orders get silver's columns and types without repeating them.
# MAGIC
# MAGIC **Re-run, not re-create.** You changed the pipeline's code, so you run the
# MAGIC pipeline again; there's nothing to delete or recreate. The update reads the
# MAGIC files as they are now, builds the new dataset, and brings the existing ones up
# MAGIC to date as usual. If you change the definition of an existing dataset, the
# MAGIC update recomputes it, then everything downstream of it. The same holds for
# MAGIC any edit you make to a pipeline's code.
# MAGIC
# MAGIC **What you should see:** `silver_customer_orders` appears in the graph with
# MAGIC edges from `silver_transactions` and `bronze_customers`, and runs after
# MAGIC `silver_transactions`, although you put no ordering anywhere. Then run the
# MAGIC check below.

# COMMAND ----------

# Check — run after the pipeline update with silver_customer_orders has finished
orders = spark.table("silver_customer_orders")

expected_columns = {"transactionID", "customerID", "first_name", "customer_country", "totalPrice"}
assert expected_columns <= set(orders.columns), f"missing columns: {expected_columns - set(orders.columns)}"
assert orders.count() == spark.table("silver_transactions").count(), \
    "silver_customer_orders should have one row per transaction; is it a LEFT JOIN on customerID?"
assert orders.filter("first_name IS NOT NULL").count() > 0, "no transaction found its customer; check the ON condition"
print("Check passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC Think back to the notebook with three cells. Adding `silver_customer_orders`
# MAGIC there would have meant a fourth cell in the right place. Here the dependency
# MAGIC *is* the `FROM` clause.
# MAGIC
# MAGIC Next: open `03-data-quality` from the file browser on the left.
