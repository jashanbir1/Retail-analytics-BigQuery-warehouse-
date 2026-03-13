import os
import random
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

from get_shopify_token import get_shopify_access_token


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN")

SHOPIFY_PRODUCTS_URL = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/2026-01/products.json?limit=250"
SHOPIFY_CUSTOMERS_URL = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/2026-01/customers.json?limit=250"
SHOPIFY_ORDERS_URL = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/2026-01/orders.json"


def validate_env() -> None:
    if not SHOPIFY_STORE_DOMAIN:
        raise ValueError("Missing SHOPIFY_STORE_DOMAIN")


def build_headers(access_token: str) -> dict:
    return {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }


def fetch_products(access_token: str) -> list[dict]:
    response = requests.get(
        SHOPIFY_PRODUCTS_URL,
        headers=build_headers(access_token),
        timeout=30,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch products.\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text}"
        )

    payload = response.json()
    return payload.get("products", [])


def fetch_customers(access_token: str) -> list[dict]:
    response = requests.get(
        SHOPIFY_CUSTOMERS_URL,
        headers=build_headers(access_token),
        timeout=30,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch customers.\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text}"
        )

    payload = response.json()
    return payload.get("customers", [])


def build_variant_pool(products: list[dict]) -> list[dict]:
    variant_pool = []

    for product in products:
        for variant in product.get("variants", []):
            price_str = variant.get("price")
            try:
                price = float(price_str) if price_str is not None else 0.0
            except ValueError:
                price = 0.0

            # Skip gift cards and zero/negative priced variants for cleaner seeded orders
            if product.get("product_type") == "giftcard":
                continue
            if price <= 0:
                continue

            variant_pool.append(
                {
                    "product_id": product["id"],
                    "product_title": product.get("title"),
                    "variant_id": variant["id"],
                    "variant_title": variant.get("title"),
                    "price": price_str,
                }
            )

    return variant_pool


def build_order_payload(customer: dict, variant_pool: list[dict]) -> dict:
    selected_variants = random.sample(
        variant_pool,
        k=min(random.randint(1, 2), len(variant_pool)),
    )

    line_items = []
    for variant in selected_variants:
        quantity = random.randint(1, 3)
        line_items.append(
            {
                "variant_id": variant["variant_id"],
                "quantity": quantity,
            }
        )

    order_payload = {
        "order": {
            "customer": {
                "id": customer["id"],
            },
            "line_items": line_items,
            "financial_status": "paid",
            "fulfillment_status": "fulfilled",
            "send_receipt": False,
            "send_fulfillment_receipt": False,
            "tags": "seeded,pipeline_test",
            "inventory_behaviour": "decrement_ignoring_policy",
        }
    }

    return order_payload


def create_order(access_token: str, order_payload: dict) -> dict:
    response = requests.post(
        SHOPIFY_ORDERS_URL,
        headers=build_headers(access_token),
        json=order_payload,
        timeout=30,
    )

    if response.status_code == 429:
        raise RuntimeError(
            "Rate limit hit while creating orders. "
            "Wait a minute and rerun with a smaller target_count."
        )

    if response.status_code not in (200, 201):
        raise RuntimeError(
            f"Failed to create order.\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text}"
        )

    payload = response.json()
    return payload["order"]

def main() -> None:
    validate_env()

    access_token = get_shopify_access_token()

    products = fetch_products(access_token)
    customers = fetch_customers(access_token)
    variant_pool = build_variant_pool(products)

    if not customers:
        raise RuntimeError("No customers found. Seed customers first.")

    if not variant_pool:
        raise RuntimeError("No usable product variants found for order seeding.")

    target_count = 2
    created = 0

    for i in range(target_count):
        customer = random.choice(customers)
        order_payload = build_order_payload(customer, variant_pool)

        try:
            order = create_order(access_token, order_payload)
        except RuntimeError as e:
            print(f"Stopped early: {e}")
            break

        created += 1
        print(
            f"Created order {created}: "
            f"order_id={order['id']} "
            f"order_name={order.get('name')} "
            f"customer_id={customer['id']}"
        )

        # Small pause between creates to be safer with dev store limits
        if i < target_count - 1:
            time.sleep(15)

    print(f"Finished. Orders created: {created}")


if __name__ == "__main__":
    main()