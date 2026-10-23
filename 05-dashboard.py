# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Optional: a dashboard on gold
# MAGIC
# MAGIC This notebook is optional and sits outside the 45 minutes; if you're short
# MAGIC on time, go straight to `06-cdc` and come back later. By the end of it
# MAGIC you'll have a one-page AI/BI dashboard on `gold_franchise_daily`, with charts
# MAGIC that Genie drafted from a plain-language prompt and you reviewed. Allow about
# MAGIC ten minutes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Put the dashboard on gold
# MAGIC
# MAGIC A dashboard is one more consumer of the pipeline, and it reads gold for the
# MAGIC same reason the queries in notebook 04 did: the joins, types and
# MAGIC aggregations are already done, and the pipeline keeps the table current. The
# MAGIC dashboard only has to query one table. When the pipeline next updates
# MAGIC `gold_franchise_daily`, the dashboard shows the new numbers the next time it
# MAGIC runs its queries; nothing in the dashboard changes.
# MAGIC
# MAGIC The dashboard needs the table's full name, because it isn't part of the
# MAGIC pipeline and has no default schema. Run the setup cell to print it:

# COMMAND ----------

import re

SCHEMA = re.sub(r"\W", "_", spark.sql("SELECT current_user()").first()[0].split("@")[0])  # your own schema, named after your user
print(f"dbacademy.{SCHEMA}.gold_franchise_daily")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the dashboard
# MAGIC
# MAGIC Open a new browser tab for the dashboard, as you did for the pipeline.
# MAGIC
# MAGIC 1. Click **New** at the top of the sidebar and select **Dashboard**. Name
# MAGIC    it `<your schema>_bakehouse_dashboard`.
# MAGIC 2. On the **Data** tab, add a dataset from the table printed above,
# MAGIC    `dbacademy.<your schema>.gold_franchise_daily`. The dataset is the query
# MAGIC    every chart on the page draws from.
# MAGIC 3. Switch to the canvas. Dashboards run their queries on a SQL warehouse;
# MAGIC    if you're asked to choose one, pick the serverless warehouse the lab
# MAGIC    provides.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let Genie draft the charts
# MAGIC
# MAGIC Instead of building the charts one by one, describe the whole page to Genie
# MAGIC in one prompt. As with the Genie Code prompt in notebook 04, the more
# MAGIC precisely you name the dataset, the columns and the aggregations, the closer
# MAGIC the first draft is to what you meant. Say also what Genie should leave alone:
# MAGIC unless told otherwise, it may change other parts of the dashboard, such as
# MAGIC its name. This prompt makes a useful first page:
# MAGIC
# MAGIC > Using only the `gold_franchise_daily` dataset, add four widgets to this
# MAGIC > page: a counter showing the sum of `revenue`; a bar chart of the sum of
# MAGIC > `revenue` by `franchise_name`, sorted from highest to lowest; a line chart
# MAGIC > of the sum of `revenue` by `sale_date`, one point per day; and a filter on
# MAGIC > `franchise_city` that applies to the three charts. Put the counter and the
# MAGIC > filter in the top row, and the bar chart and line chart below them. Don't
# MAGIC > rename the dashboard, and don't change its datasets or anything else beyond
# MAGIC > these four widgets.
# MAGIC
# MAGIC Review each widget before you keep it. Select the chart and check its
# MAGIC configuration: the field on each axis, and above all the aggregation. A
# MAGIC chart of `average_transaction_value` should average it, while `revenue`
# MAGIC and `transaction_count` should be summed; summing an average gives a number
# MAGIC that looks right and means nothing. Change anything Genie got wrong in the
# MAGIC configuration panel, or give it a more precise prompt.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish and share
# MAGIC
# MAGIC Click **Publish**. The published version is the one other people see; the
# MAGIC draft stays yours to keep editing. Open the published dashboard and try the
# MAGIC `franchise_city` filter: every chart on the page follows it.
# MAGIC
# MAGIC That's the whole path from the landing zone to a business user: bronze
# MAGIC copies the source, silver makes it trustworthy and counts what isn't, gold
# MAGIC shapes it for one question, and the dashboard shows the answer. Every layer
# MAGIC except the last is the pipeline's job.
# MAGIC
# MAGIC Next: open `06-cdc` from the file browser on the left.
