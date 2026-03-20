from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


# Absolute project paths
REPO_ROOT = "/Users/jashan/Downloads/Retail-analytics-BigQuery-warehouse-"
INGEST_PYTHON = f"{REPO_ROOT}/venv/bin/python"
DBT_BIN = f"{REPO_ROOT}/venv312/bin/dbt"
DBT_PROJECT_DIR = f"{REPO_ROOT}/shopify_retail_dbt"

DBT_PROFILES_DIR = "/Users/jashan/.dbt"


with DAG(
    dag_id="retail_warehouse_pipeline",
    description="Extract Shopify data to GCS, load bronze tables in BigQuery, run dbt transformations(bronze -> silver -> gold(fact/dimension) -> gold marts), and run dbt tests.",
    start_date=datetime(2026, 3, 20),
    schedule=None,   # manual trigger for now
    catchup=False,
    tags=["retail", "shopify", "bigquery", "dbt"],
) as dag:

    extract_products_to_gcs = BashOperator(
        task_id="extract_products_to_gcs",
        bash_command=f"cd {REPO_ROOT} && {INGEST_PYTHON} src/ingest/extract_products_to_gcs.py",
    )

    extract_customers_to_gcs = BashOperator(
        task_id="extract_customers_to_gcs",
        bash_command=f"cd {REPO_ROOT} && {INGEST_PYTHON} src/ingest/extract_customers_to_gcs.py",
    )

    extract_orders_to_gcs = BashOperator(
        task_id="extract_orders_to_gcs",
        bash_command=f"cd {REPO_ROOT} && {INGEST_PYTHON} src/ingest/extract_orders_to_gcs.py",
    )

    load_products_bronze = BashOperator(
        task_id="load_products_bronze",
        bash_command=f"cd {REPO_ROOT} && {INGEST_PYTHON} src/load/load_products_bronze.py",
    )

    load_customers_bronze = BashOperator(
        task_id="load_customers_bronze",
        bash_command=f"cd {REPO_ROOT} && {INGEST_PYTHON} src/load/load_customers_bronze.py",
    )

    load_orders_bronze = BashOperator(
        task_id="load_orders_bronze",
        bash_command=f"cd {REPO_ROOT} && {INGEST_PYTHON} src/load/load_orders_bronze.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_BIN} run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_BIN} test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # Dependencies
    extract_products_to_gcs >> load_products_bronze
    extract_customers_to_gcs >> load_customers_bronze
    extract_orders_to_gcs >> load_orders_bronze

    [load_products_bronze, load_customers_bronze, load_orders_bronze] >> dbt_run >> dbt_test