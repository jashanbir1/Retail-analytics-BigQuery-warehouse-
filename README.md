# Retail Analytics BigQuery Warehouse

An end-to-end retail analytics data engineering project that extracts Shopify data through API, lands raw snapshots in Google Cloud Storage (GCS), loads bronze tables into BigQuery, transforms data with dbt into silver and gold models, builds fact and dimension tables in a star schema, creates curated gold marts from those gold tables, visualizes business metrics in Metabase, and orchestrates the full pipeline with Apache Airflow.

## Project Overview

This project simulates a realistic modern analytics stack with a strong concentration in data engineering concepts and tools for a retail business. Following an ELT ingestion pattern, the pipeline ingests Shopify customers, orders, and products, stores raw JSON snapshots in Google Cloud Storage, loads them into BigQuery bronze tables, transforms them into cleaned silver models and gold analytics models using dbt, and surfaces KPIs in Metabase dashboards.

## What This Project Accomplishes

- Extracts Shopify customers, orders, and products into Google Cloud Storage
- Stores raw data in a date-partitioned landing zone structure in GCS
- Loads raw JSON payloads into BigQuery bronze tables
- Transforms bronze data into silver and gold models with dbt
- Builds fact and dimension tables in a star schema
- Creates curated gold marts for:
  - daily sales
  - product sales performance
  - customer lifetime value
  - order basket behavior
- Runs dbt tests to validate model quality
- Visualizes business metrics in Metabase dashboards
- Orchestrates the entire pipeline end-to-end with Apache Airflow
- Conversational AI layer allowing stakeholders to ask questions about the data

## Architecture

```
Shopify API
    ↓ extract (paginated)
GCS Raw Landing Zone (date-partitioned)
    ↓ incremental load
BigQuery Bronze
    ↓ dbt (deduplicated)
BigQuery Silver
    ↓ dbt
BigQuery Gold — Fact/Dimension Star Schema + Curated Marts
    ↓                          ↓
Metabase Dashboards     Streamlit Dashboard
                        + AI Agent (Claude)
                          Natural Language → SQL → Results
```

Airflow orchestrates:
1. Extract products, customers, and orders from Shopify to GCS
2. Incrementally load bronze tables in BigQuery (skip already-loaded partitions, refresh today)
3. Run dbt transformations
4. Run dbt tests
5. Run data quality checks

## Tech Stack

- Python
- Shopify Admin API
- Google Cloud Storage
- BigQuery
- dbt
- Apache Airflow
- Metabase
- Streamlit
- Claude (Anthropic) — AI agent with tool use
- GitHub
- Docker

## Environments

This project uses three Python environments:

1. `venv`
Used for:
- Shopify extract scripts
- BigQuery bronze load scripts

 2. `venv312`
Used for:
- dbt

3. `airflow_venv`
Used for:
- Apache Airflow

## Repository Structure
```text
airflow/
  dags/
    Data_warehouse_pipeline_dag.py

src/
  ingest/
    extract_customers_to_gcs.py
    extract_orders_to_gcs.py
    extract_products_to_gcs.py
    get_shopify_token.py

  load/
    load_customers_bronze.py
    load_orders_bronze.py
    load_products_bronze.py

shopify_retail_dbt/
  models/
    example/
      silver/
      gold/
        marts/

```
## AI Layer — Natural Language SQL Agent

Built on top of the gold marts, the Streamlit dashboard includes a conversational AI agent powered by Claude (Anthropic) that lets any user — technical or not — query the warehouse in plain English.

### How It Works

A user types a question like *"what were my top 10 products last month by revenue?"* The agent runs an observe → reason → act loop:

1. **Observe** — Claude receives the question and the full warehouse schema (all gold table definitions, columns, and what they mean) as context
2. **Reason** — Claude decides what SQL to write to answer the question
3. **Act** — Claude calls the `run_bigquery_sql` tool, which executes the SQL against BigQuery and returns real results
4. **Observe** — Claude reads the query results
5. **Reason** — Claude decides if the answer is complete or if the query needs refinement
6. **Respond** — Claude returns a plain English answer with the data

The loop runs until Claude is satisfied with the result, then surfaces the answer, the generated SQL, and a results table directly in the Streamlit UI.

### What Makes It Agentic

This is not a single prompt → response pattern. Claude controls the loop via `stop_reason`:
- `tool_use` → Claude wants to run SQL → execute it → feed results back → continue
- `end_turn` → Claude is done → return the answer to the user

Claude can run multiple queries in one turn — for example, first checking which tables are relevant, then querying, then refining if the result looks wrong.

### Cost Efficiency

The warehouse schema is passed as a cached system prompt (`cache_control: ephemeral`). The first question pays full tokenization cost. Every follow-up question in the same session reuses the cached schema at a fraction of the cost.

### Tech

- `anthropic` Python SDK
- `claude-opus-4-7` model with tool use
- `run_bigquery_sql` tool — Claude-generated SQL executed directly against BigQuery
- Streamlit chat UI with message history, SQL expander, and results dataframe



