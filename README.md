# Spark Declarative Pipelines on Lakeflow

A 45-minute, hands-on lab. Over six notebooks, plus an optional seventh, you'll
turn the Bakehouse sample tables into a declarative pipeline: bronze copies of
the sources, a typed and enriched silver layer with a data quality rule, gold
tables for franchise and product revenue, and customer tables kept in line with
a change feed. Then you'll break the rule on purpose and find the failure from
the pipeline's graph and event log.

## The notebooks

| Notebook | What it covers | Time |
| -------- | -------------- | ---- |
| `01-explore-bakehouse` — Explore Bakehouse | What a pipeline is, what Spark Declarative Pipelines does, how a job runs a pipeline, and the Bakehouse tables and their keys | 5 min |
| `02-declare-and-run` — Declare the pipeline and run it | Imperative and declarative side by side, choosing the dataset type, how a pipeline project is laid out, creating and running a pipeline, the dependency graph; you write the rest of bronze and a join | 15 min |
| `03-data-quality` — Data quality | An expectation that counts violations, where to read the counts, what warn, drop and fail do to the published data, and quarantining dropped rows | 7 min |
| `04-gold` — Gold business outputs | Two gold tables from one silver layer, one of them written with Genie Code | 6 min |
| `05-dashboard` — Optional: a dashboard on gold | A one-page AI/BI dashboard on `gold_franchise_daily`, with Genie drafting the charts | 10 min, optional |
| `06-cdc` — Track data changes | CDC and slowly changing dimensions, a customer change feed, and `AUTO CDC` into SCD type 1 and type 2 tables, compared with a hand-written `MERGE` | 7 min |
| `07-operate` — Operate and troubleshoot | Update details, lineage, the update history, a failed update, scheduling with Lakeflow Jobs, and deploying pipelines and jobs as code | 5 min |

Work through them in order; each one builds on the pipeline the previous one
left behind. `05-dashboard` is optional and outside the 45 minutes; skip it if
you're short on time and come back to it later.

## How the lab works

Each notebook mixes explanation with steps you carry out in the pipeline editor,
and exercises you complete yourself. After most exercises comes a check cell
that confirms your result with `assert`, or a query that shows you the result. If you get stuck, the
`reference/transformations` folder in the file browser on the left has the
finished version of every pipeline file.

You share this workspace with the other participants, so the pipeline publishes
into a schema of your own. Each notebook starts with a setup cell that sets the
catalog to the lab's catalog, `dbacademy`, and derives the schema from your user
name, so you don't have to set anything and nobody collides with anyone else;
your schema already exists.

## The data

Bakehouse is a sample dataset of a fictional chain of cookie franchises: sales
transactions, and the franchises and customers behind them. It's in the
`samples.bakehouse` schema, which the workspace receives as a read-only share.
In this lab it plays the role of the landing zone, the data as it arrives from
a source system: everyone reads the same source, nobody can change it, and the
pipeline writes its own tables.
