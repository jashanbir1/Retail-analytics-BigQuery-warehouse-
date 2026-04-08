from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


REPO_ROOT = "/opt/project"
DBT_PROJECT_DIR = f"{REPO_ROOT}/shopify_retail_dbt"
DBT_PROFILES_DIR = "/home/airflow/.dbt"


with DAG(
    dag_id="retail_warehouse_pipeline",
    description=(
        "Seed Shopify test data, extract Shopify data to GCS, load bronze tables "
        "in BigQuery, run dbt transformations, run dbt tests, and execute data quality checks."
    ),
    start_date=datetime(2026, 3, 20),
    schedule=None,
    catchup=False,
    tags=["retail", "shopify", "bigquery", "dbt", "data-quality"],
) as dag:

    seed_test_customers = BashOperator(
        task_id="seed_test_customers",
        bash_command=f"cd {REPO_ROOT} && python src/ingest/seed_test_customers.py",
    )

    seed_test_orders = BashOperator(
        task_id="seed_test_orders",
        bash_command=f"cd {REPO_ROOT} && python src/ingest/seed_test_orders.py",
    )

    extract_products_to_gcs = BashOperator(
        task_id="extract_products_to_gcs",
        bash_command=f"cd {REPO_ROOT} && python src/ingest/extract_products_to_gcs.py",
    )

    extract_customers_to_gcs = BashOperator(
        task_id="extract_customers_to_gcs",
        bash_command=f"cd {REPO_ROOT} && python src/ingest/extract_customers_to_gcs.py",
    )

    extract_orders_to_gcs = BashOperator(
        task_id="extract_orders_to_gcs",
        bash_command=f"cd {REPO_ROOT} && python src/ingest/extract_orders_to_gcs.py",
    )

    load_products_bronze = BashOperator(
        task_id="load_products_bronze",
        bash_command=f"cd {REPO_ROOT} && python src/load/load_products_bronze.py",
    )

    load_customers_bronze = BashOperator(
        task_id="load_customers_bronze",
        bash_command=f"cd {REPO_ROOT} && python src/load/load_customers_bronze.py",
    )

    load_orders_bronze = BashOperator(
        task_id="load_orders_bronze",
        bash_command=f"cd {REPO_ROOT} && python src/load/load_orders_bronze.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    run_data_quality_checks = BashOperator(
        task_id="run_data_quality_checks",
        bash_command=(
            f"cd {REPO_ROOT} && "
            f"python src/validation/run_data_quality_checks.py"
        ),
    )

    # Seed test data first
    seed_test_customers >> seed_test_orders

    # After seeding, extract current Shopify data
    seed_test_orders >> [
        extract_products_to_gcs,
        extract_customers_to_gcs,
        extract_orders_to_gcs,
    ]

    # Extraction -> bronze loads
    extract_products_to_gcs >> load_products_bronze
    extract_customers_to_gcs >> load_customers_bronze
    extract_orders_to_gcs >> load_orders_bronze

    # Bronze loads -> dbt -> data quality
    [load_products_bronze, load_customers_bronze, load_orders_bronze] >> dbt_run >> dbt_test >> run_data_quality_checks