import os
import random
from pathlib import Path

import requests
from dotenv import load_dotenv

from get_shopify_token import get_shopify_access_token


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN")

SHOPIFY_GRAPHQL_URL = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/2026-01/graphql.json"


FIRST_NAMES = [
    "Aman", "Priya", "Jasleen", "Rohan", "Simran",
    "David", "Maya", "Noah", "Ava", "Ethan"
]

LAST_NAMES = [
    "Mann", "Patel", "Singh", "Sharma", "Kaur",
    "Johnson", "Lee", "Brown", "Garcia", "Wilson"
]

CITIES = [
    "Los Angeles", "Toronto", "Ottawa", "Vancouver", "New York"
]


def validate_env() -> None:
    if not SHOPIFY_STORE_DOMAIN:
        raise ValueError("Missing SHOPIFY_STORE_DOMAIN")


def build_customer_input(index: int) -> dict:
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    city = random.choice(CITIES)

    email = f"{first_name.lower()}.{last_name.lower()}.{index}@example.com"
    

    return {
    "firstName": first_name,
    "lastName": last_name,
    "email": email,
    "tags": ["seeded", "pipeline_test"],
    "addresses": [
        {
            "address1": f"{random.randint(100, 9999)} Main St",
            "city": city,
            "countryCode": "US",
            "zip": f"{random.randint(10000, 99999)}",
        }
    ],
    }


def create_customer(access_token: str, customer_input: dict) -> dict:
    query = """
    mutation customerCreate($input: CustomerInput!) {
      customerCreate(input: $input) {
        customer {
          id
          firstName
          lastName
          email
        }
        userErrors {
          field
          message
        }
      }
    }
    """

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }

    response = requests.post(
        SHOPIFY_GRAPHQL_URL,
        headers=headers,
        json={
            "query": query,
            "variables": {"input": customer_input},
        },
        timeout=30,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Customer create request failed.\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text}"
        )

    payload = response.json()

    if "errors" in payload:
        raise RuntimeError(f"GraphQL errors: {payload['errors']}")

    result = payload["data"]["customerCreate"]

    if result["userErrors"]:
        raise RuntimeError(f"User errors: {result['userErrors']}")

    return result["customer"]


def main() -> None:
    validate_env()

    access_token = get_shopify_access_token()

    created = 0
    target_count = 5

    for i in range(1, target_count + 1):
        customer_input = build_customer_input(i)
        customer = create_customer(access_token, customer_input)
        created += 1
        print(
            f"Created customer {created}: "
            f"{customer['firstName']} {customer['lastName']} - {customer['email']}"
        )

    print(f"Finished. Customers created: {created}")


if __name__ == "__main__":
    main()