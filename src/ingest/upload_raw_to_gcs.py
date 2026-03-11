from pathlib import Path
from google.cloud import storage
import os

#globals
PROJECT_ROOT = Path(__file__).resolve().parents[2]

LOCAL_FILE_PATH = PROJECT_ROOT /"data"/"raw"/"Online_Retail.csv"
BUCKET_NAME = "jmann-bucket1-rdw"
DESTINATION_BLOB_NAME = "raw/online_retail_dataset/Online_Retail.csv"

# service account key path
GOOGLE_APPLICATION_CREDENTIALS = PROJECT_ROOT / "credentials" / "gcp-service-account.json"


#helper functions

#this is to help validate that the file exists
def validate_local_file(file_path: Path) -> None:
    if not file_path.exists():
        raise FileNotFoundError(f"Local file not found: {file_path}")

#function to upload file to GCS
def upload_file_to_gcs(
    bucket_name: str,
    source_file_path: Path,
    destination_blob_name: str,
) -> None:
    """Upload a local file to a GCS bucket."""
    validate_local_file(source_file_path) #make sure exists

    #used to authenticate
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GOOGLE_APPLICATION_CREDENTIALS)

    #app’s connection to my Google Cloud Storage account
    client = storage.Client()
    #Point me to my bucket called jmann-bucket1-rdw
    bucket = client.bucket(bucket_name)
    #point to where i want it to be in gcs, create a reference for the file I want to store at this path inside the bucket, blob is the destination
    blob = bucket.blob(destination_blob_name)

    #uploads my local file into that blob location.
    blob.upload_from_filename(str(source_file_path))

    print("Upload complete.")
    print(f"Local file: {source_file_path}")
    print(f"GCS path: gs://{bucket_name}/{destination_blob_name}")


def main() -> None:
    upload_file_to_gcs(
        bucket_name=BUCKET_NAME,
        source_file_path=LOCAL_FILE_PATH,
        destination_blob_name=DESTINATION_BLOB_NAME,
    )


if __name__ == "__main__":
    main()