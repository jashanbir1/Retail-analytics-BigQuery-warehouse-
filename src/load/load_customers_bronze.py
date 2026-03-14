import os
import json
from datetime import datetime, UTC
from pathlib import Path

from dotenv import load_dotenv
from google.api_core.exceptions import Conflict
from google.cloud import storage
from google.cloud import bigquery

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

GOOGLE_APPLICATION_CREDENTIALS = PROJECT_ROOT / "credentials" / "gcp-service-account.json"

PROJECT_ID = "retail-data-warehouse-project"
BRONZE_DATASET = "retail_bronze"
BRONZE_TABLE = "shopify_customers_raw"

GCS_BUCKET_NAME = "jmann-bucket1-rdw"
GCS_BLOB_NAME = "raw/shopify/customers/extract_date=2026-03-13/customers.json"

#extract raw contents from gcs raw landing zone
def get_gcs_file_contents(bucket_name: str, blob_name: str):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GOOGLE_APPLICATION_CREDENTIALS)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    file_contents = blob.download_as_text()
    json_contents = json.loads(file_contents)
    print("File contents retrieved")
    return json_contents
#build rows in uploadable format to BigQuery
def build_bronze_rows(customers_payload: dict, source_file_path: str):
    extract_date = source_file_path.split("extract_date=")[1].split("/")[0]
    ingested_at = datetime.now(UTC).isoformat()

    rows = []

    for customer in customers_payload.get("customers", []):
        rows.append(
            {
                "customer_id": str(customer["id"]),
                "extract_date": extract_date,
                "ingested_at": ingested_at,
                "source_file_path": source_file_path,
                "raw_payload": json.dumps(customer)
            }
        )
    
    return rows

#Create customers table in BigQuery if it doesnt exist 
def create_table_if_not_exists(client: bigquery.Client, table_id: str):
     
    schema = [
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
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

def load_rows_into_bigquery(client: bigquery.Client, table_id: str, rows: list[dict]):
    errors = client.insert_rows_json(table_id, rows)

    if errors:
        raise RuntimeError(f"BigQuery insert failed: {table_id}")

    print(f"Insert successful: {len(rows)} customers uploaded into {table_id}")

#run program
def main():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GOOGLE_APPLICATION_CREDENTIALS)

    bq_client = bigquery.Client(project=PROJECT_ID)

    table_id = f"{PROJECT_ID}.{BRONZE_DATASET}.{BRONZE_TABLE}"
    source_file_path = f"gs://{GCS_BUCKET_NAME}/{GCS_BLOB_NAME}"

    print("Reading raw file contents...")
    customer_payload = get_gcs_file_contents(GCS_BUCKET_NAME, GCS_BLOB_NAME)

    print("Building bronze rows...")
    customer_rows = build_bronze_rows(customer_payload, source_file_path)

    if not customer_rows:
        raise RuntimeError("No customer rows found in source payload")

    print("Creating bronze table if needed...")
    create_table_if_not_exists(bq_client,table_id)

    print("Loading rows into bronze...")
    load_rows_into_bigquery(bq_client, table_id, customer_rows)

    print("Bronze rows uploaded for customers in BigQuery")


if __name__ == "__main__":
    main()
    



