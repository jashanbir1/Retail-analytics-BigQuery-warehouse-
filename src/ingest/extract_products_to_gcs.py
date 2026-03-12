import json
import os
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.cloud import storage

from get_shopify_token import get_shopify_access_token

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN")
GCS_BUCKET_NAME = "jmann-bucket1-rdw"

GOOGLE_APPLICATION_CREDENTIALS = PROJECT_ROOT / "credentials" / "gcp-service-account.json"

SHOPIFY_PRODUCTS_URL = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/2026-01/products.json?limit=250"

def validate_env():
    missing = []

    if not SHOPIFY_STORE_DOMAIN:
        missing.append("SHOPIFY_STORE_DOMAIN")
    
    if not GCS_BUCKET_NAME:
        missing.append("GCS-BUCKET-NAME")
    
    if missing:
        raise ValueError(f"Missing required configurations: {', '.join(missing)}")


def build_gcs_blob_name() -> str:
    extract_date = date.today().isoformat()
    return f"raw/shopify/products/extract_date={extract_date}/products.json"


def fetch_products_from_shopify(access_token: str) -> dict:
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }

    response = requests.get(
        SHOPIFY_PRODUCTS_URL,
        headers=headers,
        timeout=30,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Shopify products request failed.\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text}"
        )

    return response.json()



def upload_json_to_gcs(bucket_name: str, blob_name: str, payload: dict) -> None:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GOOGLE_APPLICATION_CREDENTIALS)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_string(
        json.dumps(payload, indent=2),
        content_type="application/json",
    )

    print(f"Uploading to bucket: {bucket_name}")
    print(f"Uploading to blob: {blob_name}")

    print("Upload complete.")
    print(f"GCS path: gs://{bucket_name}/{blob_name}")


def main():
    validate_env()

    access_token = get_shopify_access_token()
    products_payload = fetch_products_from_shopify(access_token)
    blob_name = build_gcs_blob_name()

    upload_json_to_gcs(
        bucket_name = GCS_BUCKET_NAME,
        blob_name = blob_name,
        payload = products_payload,
    )

    product_count = len(products_payload.get("products",[]))
    print(f"Products extracted: {product_count}")

if __name__ == "__main__":
    main()
