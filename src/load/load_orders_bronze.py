import json
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from google.api_core.exceptions import Conflict
from google.cloud import bigquery
from google.cloud import storage

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

GOOGLE_APPLICATION_CREDENTIALS = PROJECT_ROOT / "credentials" / "gcp-service-account.json"

PROJECT_ID = "retail-data-warehouse-project"
BRONZE_DATASET = "retail_bronze"
BRONZE_TABLE = "shopify_orders_raw"

GCS_BUCKET_NAME = "jmann-bucket1-rdw"
GCS_BLOB_NAME = "raw/shopify/orders/extract_date=2026-03-13/orders.json"


def get_gcs_file_contents(bucket_name: str, blob_name: str) -> dict:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GOOGLE_APPLICATION_CREDENTIALS)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    file_contents = blob.download_as_text()
    return json.loads(file_contents)


def build_bronze_rows(orders_payload: dict, source_file_path: str) -> list[dict]:
    extract_date = source_file_path.split("extract_date=")[1].split("/")[0]
    ingested_at = datetime.utcnow().isoformat()

    rows = []

    for order in orders_payload.get("orders", []):
        rows.append(
            {
                "order_id": str(order["id"]),
                "extract_date": extract_date,
                "ingested_at": ingested_at,
                "source_file_path": source_file_path,
                "raw_payload": json.dumps(order),
            }
        )

    return rows


def create_table_if_not_exists(client: bigquery.Client, table_id: str) -> None:
    schema = [
        bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("extract_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("source_file_path", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("raw_payload", "STRING", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    try:
        client.create_table(table)
        print(f"Created table: {table_id}")
    except Conflict:
        print(f"Table already exists: {table_id}")


def load_rows_to_bigquery(client: bigquery.Client, table_id: str, rows: list[dict]) -> None:
    errors = client.insert_rows_json(table_id, rows)

    if errors:
        raise RuntimeError(f"BigQuery insert failed: {errors}")

    print(f"Inserted {len(rows)} rows into {table_id}")


def main() -> None:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GOOGLE_APPLICATION_CREDENTIALS)

    bq_client = bigquery.Client(project=PROJECT_ID)

    table_id = f"{PROJECT_ID}.{BRONZE_DATASET}.{BRONZE_TABLE}"
    source_file_path = f"gs://{GCS_BUCKET_NAME}/{GCS_BLOB_NAME}"

    print("Reading raw orders file from GCS...")
    orders_payload = get_gcs_file_contents(GCS_BUCKET_NAME, GCS_BLOB_NAME)

    print("Building bronze rows...")
    bronze_rows = build_bronze_rows(orders_payload, source_file_path)

    if not bronze_rows:
        raise RuntimeError("No order rows found in source payload.")

    print("Creating bronze table if needed...")
    create_table_if_not_exists(bq_client, table_id)

    print("Loading rows into BigQuery...")
    load_rows_to_bigquery(bq_client, table_id, bronze_rows)

    print("Bronze load complete.")


if __name__ == "__main__":
    main()