from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from google.cloud import bigquery


PROJECT_ID = "retail-data-warehouse-project"
BRONZE_DATASET = "retail_bronze"
MONITORING_DATASET = "retail_monitoring"
MONITORING_TABLE = "data_quality_results"

# Thresholds — under these rates: warn, log, continue. Over: hard fail, stop pipeline.
NULL_ID_FAIL_THRESHOLD = 0.05    # 5% of today's rows
DUPLICATE_FAIL_THRESHOLD = 0.03  # 3% of today's rows

TABLE_CONFIGS = {
    "orders": {
        "table": "shopify_orders_raw",
        "id_column": "order_id",
    },
    "customers": {
        "table": "shopify_customers_raw",
        "id_column": "customer_id",
    },
    "products": {
        "table": "shopify_products_raw",
        "id_column": "product_id",
    },
}


def load_local_env(env_path: str = ".env") -> None:
    env_file = Path(env_path)
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


load_local_env()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def run_scalar_query(client: bigquery.Client, query: str) -> Any:
    rows = list(client.query(query).result())
    if not rows:
        return None
    return rows[0][0]


def run_list_query(client: bigquery.Client, query: str) -> list:
    return [row[0] for row in client.query(query).result()]


@dataclass
class CheckResult:
    run_id: str
    run_timestamp: datetime
    check_timestamp: datetime
    check_name: str
    layer_name: str
    table_name: str
    metric_name: str
    metric_value: Optional[float]
    threshold_value: Optional[str]
    status: str
    severity: Optional[str]
    details: Optional[str]
    ai_prompt: Optional[str]
    ai_explanation: Optional[str]
    likely_causes: Optional[str]
    suggested_actions: Optional[str]


def build_result(
    *,
    run_id: str,
    run_timestamp: datetime,
    check_name: str,
    layer_name: str,
    table_name: str,
    metric_name: str,
    metric_value: Optional[float],
    threshold_value: Optional[str],
    status: str,
    severity: Optional[str],
    details: Optional[str],
) -> CheckResult:
    return CheckResult(
        run_id=run_id,
        run_timestamp=run_timestamp,
        check_timestamp=utc_now(),
        check_name=check_name,
        layer_name=layer_name,
        table_name=table_name,
        metric_name=metric_name,
        metric_value=metric_value,
        threshold_value=threshold_value,
        status=status,
        severity=severity,
        details=details,
        ai_prompt=None,
        ai_explanation=None,
        likely_causes=None,
        suggested_actions=None,
    )


def insert_results(client: bigquery.Client, results: list[CheckResult]) -> None:
    table_id = f"{PROJECT_ID}.{MONITORING_DATASET}.{MONITORING_TABLE}"
    rows_to_insert = []
    for result in results:
        row = asdict(result)
        row["run_timestamp"] = result.run_timestamp.isoformat()
        row["check_timestamp"] = result.check_timestamp.isoformat()
        rows_to_insert.append(row)
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        raise RuntimeError(f"Failed to insert bronze check results: {errors}")


# ── Check 1: Row count ──────────────────────────────────────────────────────

def check_row_count(
    *,
    client: bigquery.Client,
    run_id: str,
    run_timestamp: datetime,
    entity: str,
    table: str,
    today: str,
) -> CheckResult:
    count = run_scalar_query(
        client,
        f"""
        SELECT COUNT(*)
        FROM `{PROJECT_ID}.{BRONZE_DATASET}.{table}`
        WHERE CAST(extract_date AS STRING) = '{today}'
        """,
    )
    row_count = int(count) if count is not None else 0

    if row_count == 0:
        status, severity = "fail", "high"
        details = f"No rows found for extract_date={today} in {table}. Pipeline stopped."
    else:
        status, severity = "pass", "low"
        details = f"{row_count} rows found for extract_date={today} in {table}."

    return build_result(
        run_id=run_id,
        run_timestamp=run_timestamp,
        check_name=f"bronze_{entity}_row_count_check",
        layer_name="bronze",
        table_name=table,
        metric_name="today_row_count",
        metric_value=float(row_count),
        threshold_value="> 0 rows required",
        status=status,
        severity=severity,
        details=details,
    )


# ── Check 2: Extract date matches today ────────────────────────────────────

def check_extract_date(
    *,
    client: bigquery.Client,
    run_id: str,
    run_timestamp: datetime,
    entity: str,
    table: str,
    today: str,
) -> CheckResult:
    latest = run_scalar_query(
        client,
        f"""
        SELECT CAST(MAX(extract_date) AS STRING)
        FROM `{PROJECT_ID}.{BRONZE_DATASET}.{table}`
        """,
    )

    if latest != today:
        status, severity = "fail", "high"
        details = f"Latest extract_date in {table} is {latest}, expected {today}. Pipeline stopped."
    else:
        status, severity = "pass", "low"
        details = f"extract_date={today} confirmed in {table}."

    return build_result(
        run_id=run_id,
        run_timestamp=run_timestamp,
        check_name=f"bronze_{entity}_extract_date_check",
        layer_name="bronze",
        table_name=table,
        metric_name="extract_date_matches_today",
        metric_value=1.0 if status == "pass" else 0.0,
        threshold_value=f"extract_date must equal {today}",
        status=status,
        severity=severity,
        details=details,
    )


# ── Check 3: Null IDs ───────────────────────────────────────────────────────

def check_null_ids(
    *,
    client: bigquery.Client,
    run_id: str,
    run_timestamp: datetime,
    entity: str,
    table: str,
    id_column: str,
    today: str,
) -> tuple[CheckResult, bool]:
    """Returns (CheckResult, should_hard_fail)."""
    total = run_scalar_query(
        client,
        f"""
        SELECT COUNT(*)
        FROM `{PROJECT_ID}.{BRONZE_DATASET}.{table}`
        WHERE CAST(extract_date AS STRING) = '{today}'
        """,
    ) or 0

    null_count = run_scalar_query(
        client,
        f"""
        SELECT COUNT(*)
        FROM `{PROJECT_ID}.{BRONZE_DATASET}.{table}`
        WHERE CAST(extract_date AS STRING) = '{today}'
          AND {id_column} IS NULL
        """,
    ) or 0

    null_rate = float(null_count) / float(total) if total > 0 else 0.0
    null_pct = round(null_rate * 100, 2)

    hard_fail = null_rate > NULL_ID_FAIL_THRESHOLD

    if null_count == 0:
        status, severity = "pass", "low"
        details = f"No null {id_column} values found in today's extract."
    elif hard_fail:
        status, severity = "fail", "high"
        details = (
            f"Found {null_count} null {id_column} values ({null_pct}%) in {table} "
            f"for extract_date={today}. Exceeds {NULL_ID_FAIL_THRESHOLD*100:.0f}% threshold. Pipeline stopped."
        )
    else:
        status, severity = "warn", "medium"
        details = (
            f"Found {null_count} null {id_column} values ({null_pct}%) in {table} "
            f"for extract_date={today}. Under threshold — flagged and skipped."
        )

    result = build_result(
        run_id=run_id,
        run_timestamp=run_timestamp,
        check_name=f"bronze_{entity}_null_id_check",
        layer_name="bronze",
        table_name=table,
        metric_name=f"null_{id_column}_rate",
        metric_value=round(null_rate, 4),
        threshold_value=f"fail if > {NULL_ID_FAIL_THRESHOLD*100:.0f}%; warn if any nulls under threshold",
        status=status,
        severity=severity,
        details=details,
    )
    return result, hard_fail


# ── Check 4: Intra-partition duplicates ────────────────────────────────────

def check_intrapartition_duplicates(
    *,
    client: bigquery.Client,
    run_id: str,
    run_timestamp: datetime,
    entity: str,
    table: str,
    id_column: str,
    today: str,
) -> tuple[CheckResult, bool]:
    """Returns (CheckResult, should_hard_fail)."""
    total = run_scalar_query(
        client,
        f"""
        SELECT COUNT(*)
        FROM `{PROJECT_ID}.{BRONZE_DATASET}.{table}`
        WHERE CAST(extract_date AS STRING) = '{today}'
        """,
    ) or 0

    dup_count = run_scalar_query(
        client,
        f"""
        SELECT COUNT(*)
        FROM (
          SELECT {id_column}
          FROM `{PROJECT_ID}.{BRONZE_DATASET}.{table}`
          WHERE CAST(extract_date AS STRING) = '{today}'
          GROUP BY {id_column}
          HAVING COUNT(*) > 1
        )
        """,
    ) or 0

    # Fetch the actual duplicate IDs (up to 20) to surface in Streamlit
    dup_ids = run_list_query(
        client,
        f"""
        SELECT {id_column}
        FROM `{PROJECT_ID}.{BRONZE_DATASET}.{table}`
        WHERE CAST(extract_date AS STRING) = '{today}'
        GROUP BY {id_column}
        HAVING COUNT(*) > 1
        LIMIT 20
        """,
    )

    dup_rate = float(dup_count) / float(total) if total > 0 else 0.0
    dup_pct = round(dup_rate * 100, 2)
    hard_fail = dup_rate > DUPLICATE_FAIL_THRESHOLD

    ids_preview = ", ".join(str(i) for i in dup_ids[:10]) if dup_ids else ""

    if dup_count == 0:
        status, severity = "pass", "low"
        details = f"No intra-partition duplicate {id_column} values found in today's extract."
    elif hard_fail:
        status, severity = "fail", "high"
        details = (
            f"Found {dup_count} duplicate {id_column} values ({dup_pct}%) in {table} "
            f"for extract_date={today}. Exceeds {DUPLICATE_FAIL_THRESHOLD*100:.0f}% threshold. "
            f"Pipeline stopped. IDs: [{ids_preview}]"
        )
    else:
        status, severity = "warn", "medium"
        details = (
            f"Found {dup_count} duplicate {id_column} values ({dup_pct}%) in {table} "
            f"for extract_date={today}. Under threshold — flagged and skipped. "
            f"IDs: [{ids_preview}]"
        )

    result = build_result(
        run_id=run_id,
        run_timestamp=run_timestamp,
        check_name=f"bronze_{entity}_duplicate_id_check",
        layer_name="bronze",
        table_name=table,
        metric_name=f"intrapartition_duplicate_{id_column}_rate",
        metric_value=round(dup_rate, 4),
        threshold_value=f"fail if > {DUPLICATE_FAIL_THRESHOLD*100:.0f}%; warn if any duplicates under threshold",
        status=status,
        severity=severity,
        details=details,
    )
    return result, hard_fail


# ── Main ────────────────────────────────────────────────────────────────────

def run_checks_for_entity(entity: str) -> None:
    if entity not in TABLE_CONFIGS:
        raise ValueError(f"Unknown entity '{entity}'. Choose from: {list(TABLE_CONFIGS.keys())}")

    config = TABLE_CONFIGS[entity]
    table = config["table"]
    id_column = config["id_column"]
    today = date.today().isoformat()

    client = get_bq_client()
    run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID", f"manual_bronze_check_{utc_now().isoformat()}")
    run_timestamp = utc_now()

    results: list[CheckResult] = []
    hard_failures: list[str] = []

    # Check 1: Row count
    row_count_result = check_row_count(
        client=client, run_id=run_id, run_timestamp=run_timestamp,
        entity=entity, table=table, today=today,
    )
    results.append(row_count_result)
    if row_count_result.status == "fail":
        hard_failures.append(row_count_result.check_name)

    # Check 2: Extract date
    extract_date_result = check_extract_date(
        client=client, run_id=run_id, run_timestamp=run_timestamp,
        entity=entity, table=table, today=today,
    )
    results.append(extract_date_result)
    if extract_date_result.status == "fail":
        hard_failures.append(extract_date_result.check_name)

    # Check 3: Null IDs
    null_result, null_hard_fail = check_null_ids(
        client=client, run_id=run_id, run_timestamp=run_timestamp,
        entity=entity, table=table, id_column=id_column, today=today,
    )
    results.append(null_result)
    if null_hard_fail:
        hard_failures.append(null_result.check_name)

    # Check 4: Intra-partition duplicates
    dup_result, dup_hard_fail = check_intrapartition_duplicates(
        client=client, run_id=run_id, run_timestamp=run_timestamp,
        entity=entity, table=table, id_column=id_column, today=today,
    )
    results.append(dup_result)
    if dup_hard_fail:
        hard_failures.append(dup_result.check_name)

    # Always log results to monitoring table before deciding whether to fail
    insert_results(client, results)

    for result in results:
        print(
            f"{result.check_name}: status={result.status} | "
            f"metric_value={result.metric_value} | {result.details}"
        )

    # Hard fail after logging so results are always visible in Streamlit
    if hard_failures:
        raise RuntimeError(
            f"Bronze checks hard-failed for {entity}. "
            f"Failing checks: {hard_failures}. "
            f"Results logged to monitoring table. Pipeline stopped."
        )

    print(f"Bronze checks complete for {entity}. Pipeline may proceed.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run bronze data quality checks.")
    parser.add_argument(
        "--entity",
        required=True,
        choices=["orders", "customers", "products"],
        help="Which bronze entity to check.",
    )
    args = parser.parse_args()
    run_checks_for_entity(args.entity)


if __name__ == "__main__":
    main()
