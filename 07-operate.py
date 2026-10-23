# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Operate and troubleshoot
# MAGIC
# MAGIC By the end of this notebook you'll know where to find what happened in an
# MAGIC update (status, timings, quality results, lineage), you'll have broken a
# MAGIC rule on purpose, found the failure, and repaired it, and you'll know how a
# MAGIC pipeline is scheduled and deployed in production.

# COMMAND ----------

import re

SCHEMA = re.sub(r"\W", "_", spark.sql("SELECT current_user()").first()[0].split("@")[0])  # your own schema, named after your user
spark.catalog.setCurrentCatalog("dbacademy")
spark.catalog.setCurrentDatabase(SCHEMA)
print(f"Your schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read an update in the pipeline editor
# MAGIC
# MAGIC Every run of the pipeline is an **update**. Open your pipeline and look at the
# MAGIC last one:
# MAGIC
# MAGIC - **Status per dataset.** The graph colours each dataset by its outcome in
# MAGIC   the selected update, so a failure stands out without reading any code.
# MAGIC - **Rows and timing.** The list of tables below the graph shows, per dataset,
# MAGIC   how many rows it wrote and how long it took.
# MAGIC - **Data quality.** Select `silver_transactions` for the passed and failed
# MAGIC   counts of `typical_basket`, as in notebook 03.
# MAGIC
# MAGIC **Lineage** lives in Unity Catalog, not in the pipeline. In Catalog Explorer,
# MAGIC open `dbacademy`, then your schema, then `gold_franchise_daily`, and select
# MAGIC the *Lineage* tab. It shows the tables upstream of it, back through silver and
# MAGIC bronze. That's the view you need when someone asks "where does this number
# MAGIC come from?" or "what breaks if I change this table?".

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the update history
# MAGIC
# MAGIC In notebook 03 you read expectation results from the event log. The same log
# MAGIC holds the history of every update. Each click of **Run pipeline** starts one
# MAGIC update, and `origin.update_id` identifies it. An update logs an
# MAGIC `update_progress` event every time its state changes (`QUEUED`,
# MAGIC `INITIALIZING`, `RUNNING` and so on), so this query keeps only the final
# MAGIC states, which gives one row per update:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT timestamp, origin.update_id, details:update_progress:state AS state
# MAGIC FROM event_log(TABLE(silver_transactions))
# MAGIC WHERE event_type = 'update_progress'
# MAGIC   AND details:update_progress:state IN ('COMPLETED', 'FAILED', 'CANCELED')
# MAGIC ORDER BY timestamp DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Because this is just a query, you can schedule it, chart it, or attach a
# MAGIC Databricks SQL alert that notifies you when an update fails or the share of
# MAGIC failed records crosses a threshold. We don't build the alert in this lab.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 7.1 — Break a rule, find it, repair it
# MAGIC
# MAGIC Pretend a well-meant change goes wrong: someone decides bulk sales are
# MAGIC always an error and makes `typical_basket` critical.
# MAGIC
# MAGIC 1. In `transformations/silver.sql`, change the `typical_basket`
# MAGIC    line to:
# MAGIC    ```sql
# MAGIC    CONSTRAINT typical_basket EXPECT (quantity BETWEEN 1 AND 20) ON VIOLATION FAIL UPDATE
# MAGIC    ```
# MAGIC 2. Run the pipeline.
# MAGIC 3. The update fails, because the bulk sales flagged in notebook 03 now
# MAGIC    violate a critical rule. In the graph, `silver_transactions` is marked as
# MAGIC    failed, and the datasets downstream of it weren't refreshed. Open the
# MAGIC    error for `silver_transactions`: it names the expectation that failed.
# MAGIC 4. Run the two cells below while the rule is still broken.
# MAGIC
# MAGIC **What you should see:** the error rows in the event log carry the same
# MAGIC message as the editor, and the gold table still answers queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT timestamp, message, error
# MAGIC FROM event_log(TABLE(silver_transactions))
# MAGIC WHERE level = 'ERROR'
# MAGIC ORDER BY timestamp DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows_still_served FROM gold_franchise_daily

# COMMAND ----------

# MAGIC %md
# MAGIC That second result is what notebook 03 described: the failed update didn't
# MAGIC publish anything, so readers keep the last good version instead of a
# MAGIC half-built one. You found the problem from the graph and the event log,
# MAGIC without reading a single transformation.
# MAGIC
# MAGIC 5. Restore the rule to a warning by removing `ON VIOLATION FAIL UPDATE`, and
# MAGIC    run the pipeline again. Then run the check below.

# COMMAND ----------

# Check — run after the repaired pipeline update has finished
latest = spark.sql("""
    SELECT details:update_progress:state AS state
    FROM event_log(TABLE(silver_transactions))
    WHERE event_type = 'update_progress'
    ORDER BY timestamp DESC
    LIMIT 1
""").first().state
assert latest == "COMPLETED", f"the latest update ended in state {latest}; is typical_basket a warning again?"
assert spark.table("silver_transactions").count() == spark.table("bronze_transactions").count(), \
    "silver has fewer rows than bronze; typical_basket should warn, not drop"
print("Check passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schedule the pipeline with Lakeflow Jobs
# MAGIC
# MAGIC So far you've started every update by hand. In production nobody clicks
# MAGIC **Run pipeline**; a job does. This is where the distinction from notebook 01
# MAGIC pays off: the pipeline decides *how* its datasets are built and in what
# MAGIC order, and the job decides *when* the pipeline runs and what happens around
# MAGIC it.
# MAGIC
# MAGIC A job with a single **Pipeline** task that points at your pipeline is enough
# MAGIC to run it unattended. The job then adds what the pipeline doesn't have:
# MAGIC
# MAGIC - **Triggers.** A schedule (every night at two, every hour), or an event
# MAGIC   such as new files arriving in a volume or a source table being updated, so
# MAGIC   the pipeline runs when there's something new to process.
# MAGIC - **Retries, timeouts and notifications.** A failed update can be retried
# MAGIC   automatically, and someone gets an email or a message when it still fails,
# MAGIC   such as the `FAIL UPDATE` you just caused.
# MAGIC - **Other tasks around the pipeline.** A notebook that checks the gold
# MAGIC   tables before anyone is told they're ready, or a task that refreshes a
# MAGIC   dashboard, set to start only once the pipeline task has succeeded.
# MAGIC
# MAGIC The job doesn't repeat any of the pipeline's logic. Its pipeline task knows
# MAGIC nothing about bronze, silver or gold; it starts an update, and the pipeline
# MAGIC does the rest exactly as it did when you clicked the button. We don't create a
# MAGIC job in this workshop, so nothing keeps running after it ends.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy pipelines and jobs as code
# MAGIC
# MAGIC You created the pipeline by clicking through a form, which is fine for
# MAGIC learning but hard to repeat exactly in another workspace. In a real project
# MAGIC the pipeline's settings, and the job that schedules it, live in version
# MAGIC control next to the SQL, as infrastructure as code, and a deployment creates
# MAGIC or updates both from them.
# MAGIC
# MAGIC On Databricks the usual way to do that is **Declarative Automation Bundles**.
# MAGIC A bundle is a project folder with a `databricks.yml` file that describes its
# MAGIC resources, such as pipelines, jobs and dashboards, and the *targets* they're
# MAGIC deployed to, such as development and production. For this workshop's
# MAGIC pipeline and a nightly job that runs it, the file could look like this (a
# MAGIC sketch, not a file in the lab):
# MAGIC
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: bakehouse
# MAGIC
# MAGIC variables:
# MAGIC   catalog:
# MAGIC     description: The catalog the pipeline publishes into
# MAGIC
# MAGIC resources:
# MAGIC   pipelines:
# MAGIC     bakehouse_pipeline:
# MAGIC       name: bakehouse_pipeline
# MAGIC       catalog: ${var.catalog}
# MAGIC       schema: bakehouse
# MAGIC       serverless: true
# MAGIC       root_path: .
# MAGIC       libraries:
# MAGIC         - glob:
# MAGIC             include: ./transformations/**
# MAGIC
# MAGIC   jobs:
# MAGIC     bakehouse_nightly:
# MAGIC       name: bakehouse_nightly
# MAGIC       schedule:
# MAGIC         quartz_cron_expression: '0 0 2 * * ?' # every night at two
# MAGIC         timezone_id: UTC
# MAGIC       tasks:
# MAGIC         - task_key: refresh_pipeline
# MAGIC           pipeline_task:
# MAGIC             pipeline_id: ${resources.pipelines.bakehouse_pipeline.id}
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development
# MAGIC     default: true
# MAGIC     variables:
# MAGIC       catalog: dev
# MAGIC   prod:
# MAGIC     mode: production
# MAGIC     variables:
# MAGIC       catalog: prod
# MAGIC ```
# MAGIC
# MAGIC The pipeline's settings are the ones you entered in the form: a name, the
# MAGIC default catalog and schema, serverless compute, and the `transformations`
# MAGIC folder as source code. The job refers to the pipeline by its resource name, so
# MAGIC it always runs the pipeline deployed with it. Each target sets its own
# MAGIC catalog, and because the SQL refers to tables by bare name, as you saw in
# MAGIC notebook 02, deploying to another target changes where the tables are
# MAGIC published without changing a line of SQL.
# MAGIC
# MAGIC You deploy with the Databricks CLI (`databricks bundle deploy`, with
# MAGIC `--target prod` for production), typically from a CI/CD pipeline after a
# MAGIC change is reviewed and merged. Teams that already manage their cloud with
# MAGIC Terraform can use the Databricks Terraform provider instead.
