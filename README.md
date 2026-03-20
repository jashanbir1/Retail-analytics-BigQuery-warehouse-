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

## Architecture

Shopify -> GCS Raw Landing Zone -> BigQuery Bronze -> dbt Silver -> dbt Gold (Fact/Dimension Star Schema) -> Gold Marts -> Metabase Dashboards

Airflow orchestrates:
1. extract products, customers, and orders from Shopify to GCS
2. load bronze tables in BigQuery
3. run dbt transformations
4. run dbt tests

## Tech Stack

- Python
- Shopify Admin API
- Google Cloud Storage
- BigQuery
- dbt
- Apache Airflow
- Metabase
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

