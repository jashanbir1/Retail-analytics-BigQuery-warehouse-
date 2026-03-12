from google.cloud import bigquery
from google.api_core.exceptions import Conflict
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOOGLE_APPLICATION_CREDENTIALS = PROJECT_ROOT / "credentials" / "gcp-service-account.json"

PROJECT_ID = "retail-data-warehouse-project"
BQ_LOCATION = "us-west2"   # use US if your BigQuery datasets are multi-region US

#this is the list of the names of datasets I want to create
DATASETS = [
    "retail_bronze",
    "retail_silver",
    "retail_gold",
]

#helper functions
"""""
creates the dataset in BQ: client is me (account holder), project_id is the name of the project in BQ, dataset_name is
the name of the dataset from the global DATASETS, and location is the location of where we are(us-west2)

"""""
def create_dataset(client: bigquery.Client, project_id: str, dataset_name: str, location: str) -> None:
    dataset_id = f"{project_id}.{dataset_name}" #builds full dataset_id (e.g: retail-data-warehouse-project.retail_bronze)
    dataset = bigquery.Dataset(dataset_id) #references dataset_id above, Dataset is the blueprint/type that Google provides for representing a BigQuery dataset in Python.
    dataset.location = location #set location of dataset to location, passed in as parameter

    #project_id, dataset_name, location, get saved into dataset as an object
    try:
        #then, using client, uses dataset, to create the dataset, lapping through each name in DATASETS for the name
        client.create_dataset(dataset, timeout=30) #THIS IS BigQuery API creating a data set, not my function
        print(f"Created dataset: {dataset_id}")
    except Conflict:
        print(f"Dataset already exists: {dataset_id}")


def main() -> None:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GOOGLE_APPLICATION_CREDENTIALS)

    client = bigquery.Client(project=PROJECT_ID) #Connect me to BigQuery for this GCP project so I can start doing BigQuery operations.

    for dataset_name in DATASETS:
        create_dataset(client, PROJECT_ID, dataset_name, BQ_LOCATION)
    
    print("Datasets created in BigQuery")


if __name__ == "__main__":
    main()