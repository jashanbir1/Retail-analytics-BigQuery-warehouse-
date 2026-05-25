import json
import os
from datetime import date, datetime
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
GCS_PREFIX = "raw/shopify/orders/"


def list_gcs_partition_blobs(bucket_name: str, prefix: str) -> list[str]:
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs if blob.name.endswith(".json")]


def get_loaded_extract_dates(client: bigquery.Client, table_id: str) -> set[str]:
    try:
        rows = client.query(
            f"SELECT DISTINCT CAST(extract_date AS STRING) AS extract_date FROM `{table_id}`"
        ).result()
        return {row.extract_date for row in rows}
    except Exception:
        return set()


def delete_partition(client: bigquery.Client, table_id: str, extract_date: str) -> None:
    client.query(
        f"DELETE FROM `{table_id}` WHERE CAST(extract_date AS STRING) = '{extract_date}'"
    ).result()
    print(f"  Deleted existing rows for extract_date={extract_date}")


def get_gcs_file_contents(bucket_name: str, blob_name: str) -> dict:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return json.loads(blob.download_as_text())


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


def append_rows_to_bigquery(client: bigquery.Client, table_id: str, rows: list[dict]) -> None:
    schema = [
        bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("extract_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("source_file_path", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("raw_payload", "STRING", mode="REQUIRED"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",
    )
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()
    print(f"Appended {len(rows)} orders into {table_id}")


def main() -> None:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GOOGLE_APPLICATION_CREDENTIALS)

    bq_client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{BRONZE_DATASET}.{BRONZE_TABLE}"
    today = date.today().isoformat()

    create_table_if_not_exists(bq_client, table_id)

    already_loaded = get_loaded_extract_dates(bq_client, table_id)
    print(f"Already loaded extract dates: {sorted(already_loaded)}")

    blob_names = list_gcs_partition_blobs(GCS_BUCKET_NAME, GCS_PREFIX)

    blobs_to_load = []
    for blob_name in blob_names:
        partition_date = blob_name.split("extract_date=")[1].split("/")[0]
        if partition_date == today:
            # Always reload today — new orders may have been created since last run
            if partition_date in already_loaded:
                delete_partition(bq_client, table_id, partition_date)
            blobs_to_load.append(blob_name)
        elif partition_date not in already_loaded:
            # Historical date not yet loaded — load it once
            blobs_to_load.append(blob_name)
        else:
            print(f"  Skipping {partition_date} (already loaded)")

    if not blobs_to_load:
        print("No new partitions to load. Bronze is up to date.")
        return

    print(f"Loading: {blobs_to_load}")
    all_rows = []
    for blob_name in blobs_to_load:
        source_file_path = f"gs://{GCS_BUCKET_NAME}/{blob_name}"
        payload = get_gcs_file_contents(GCS_BUCKET_NAME, blob_name)
        rows = build_bronze_rows(payload, source_file_path)
        all_rows.extend(rows)
        print(f"  {blob_name}: {len(rows)} orders")

    append_rows_to_bigquery(bq_client, table_id, all_rows)
    print("Bronze load complete.")


if __name__ == "__main__":
    main()
