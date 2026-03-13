import os
import json #read and write json
from datetime import datetime #for creating load timestamp
from pathlib import Path ##helps locate files like .env and credentials

from dotenv import load_dotenv #load values from .env
from google.api_core.exceptions import Conflict #used to catch table exists already
from google.cloud import bigquery #talks to bigquery
from google.cloud import storage #talks to GCS

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

GOOGLE_APPLICATION_CREDENTIALS = PROJECT_ROOT / "credentials" / "gcp-service-account.json"

PROJECT_ID = "retail-data-warehouse-project"
BRONZE_DATASET = "retail_bronze"
BRONZE_TABLE = "shopify_products_raw"

GCS_BUCKET_NAME = "jmann-bucket1-rdw"
GCS_BLOB_NAME = "raw/shopify/products/extract_date=2026-03-12/products.json"


#“Go to GCS, download the raw JSON file, and give it back as Python data.”
def get_gcs_file_contents(bucket_name: str, blob_name: str) -> dict:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GOOGLE_APPLICATION_CREDENTIALS) #instills the environment

    storage_client = storage.Client() #get client account info from gcs bucket, creates connection to gcs 
    bucket = storage_client.bucket(bucket_name) #gets gcs bucket jmann-bucket1-rdw
    blob = bucket.blob(blob_name) #blob name points to file in bucket

    file_contents = blob.download_as_text() #download the file in the blob as text. At this point, file_contents is a string like: {"products": [...]}
    return json.loads(file_contents) #convert it to json (python data)

#Take the Shopify products payload and turn it into BigQuery bronze rows with metadata.
def build_bronze_rows(products_payload: dict, source_file_path: str) -> list[dict]:
    extract_date = source_file_path.split("extract_date=")[1].split("/")[0] #extract date from file path(e.g "2026-03-13")
    ingested_at = datetime.utcnow().isoformat() #gives exact time the load script went at (e.g "2026-03-13T21:15:42.123456")

    rows = [] #holds bronze rows for BigQuery

    for product in products_payload.get("products", []): #grabs list under products and loops through each one, if products is empty it uses an empty list instead of crashing 
        rows.append(
            {
                "product_id": str(product["id"]),
                "extract_date": extract_date,
                "ingested_at": ingested_at,
                "source_file_path": source_file_path,
                "raw_payload": json.dumps(product), #BigQuery is storing the full original product JSON as a string.
            }
        )

    return rows #return a python list of dictionairies

#Make bronze table if it doesnt exist already
def create_table_if_not_exists(client: bigquery.Client, table_id: str) -> None:
    """
    Define schema:
    This defines the table columns and types.
    product_id → string
    extract_date → date
    ingested_at → timestamp
    source_file_path → string
    raw_payload → string
    """

    schema = [
        bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("extract_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("source_file_path", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("raw_payload", "STRING", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_id, schema=schema) #builds table object in python, does not create it in bigquery yet, only defines what the table should look like

    try:
        client.create_table(table) #sends the create-table request to BigQuery.
        print(f"Created table: {table_id}")
    except Conflict:
        print(f"Table already exists: {table_id}") #If the table already exists, the script does not fail, It just prints a message and continues.


def load_rows_to_bigquery(client: bigquery.Client, table_id: str, rows: list[dict]) -> None:
    """
    This sends all rows to BigQuery.

    It inserts JSON-style Python dictionaries into the table.
    """
    errors = client.insert_rows_json(table_id, rows)

    if errors: #If BigQuery returns row errors, stop and show them.
        raise RuntimeError(f"BigQuery insert failed: {errors}")

    print(f"Inserted {len(rows)} rows into {table_id}")



def main() -> None:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GOOGLE_APPLICATION_CREDENTIALS) #set up environment

    bq_client = bigquery.Client(project=PROJECT_ID) #connect python to my BigQuery account

    table_id = f"{PROJECT_ID}.{BRONZE_DATASET}.{BRONZE_TABLE}" #create a table id to insert data
    source_file_path = f"gs://{GCS_BUCKET_NAME}/{GCS_BLOB_NAME}" #file path of where the source data is 

    print("Reading raw products file from GCS...")
    products_payload = get_gcs_file_contents(GCS_BUCKET_NAME, GCS_BLOB_NAME) #get payload data using get_gcs_file_contents passing in the bucket name and blob name 

    print("Building bronze rows...")
    bronze_rows = build_bronze_rows(products_payload, source_file_path) #build bronze rows returning rows list of dictionairies using payload data and where the source data is 

    if not bronze_rows:
        raise RuntimeError("No product rows found in source payload.") #if raw file has no products just fail early 

    print("Creating bronze table if needed...")
    create_table_if_not_exists(bq_client, table_id) #pass in client connector and the table id and create a bronze table

    print("Loading rows into BigQuery...")
    load_rows_to_bigquery(bq_client, table_id, bronze_rows) #load rows into created bronze table

    print("Bronze load complete.")


if __name__ == "__main__":
    main()