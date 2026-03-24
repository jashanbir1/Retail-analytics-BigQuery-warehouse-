from __future__ import annotations

import os
from dataclasses import dataclass, asdict #used to create a clean result object for each quality check
from datetime import datetime, timezone
from typing import Any, Optional #type hints so code is easier to read

from google.cloud import bigquery #BigQuery client: lets bigquery sql run queries,insert rows into a bigquery table


PROJECT_ID = "retail-data-warehouse-project"
BRONZE_DATASET = "retail_bronze"
GOLD_DATASET = "retail_gold"
MONITORING_DATASET = "retail_monitoring"
MONITORING_TABLE = "data_quality_results"


"""
What @dataclass is
This is like a template for one row in the monitoring table.
Each check I run will produce one CheckResult.
For example:
	•	bronze_orders_freshness_check
	•	duplicate_order_id_check
	•	gold_daily_sales_anomaly_check

It is basically saying:
“Every quality check result should look like this.”
"""
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

#returns current time
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

#returns BigQuery client through bigquery connection, me
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)

"""
What it does:
Runs a SQL query and returns the first value of the first row.

If your query is:
SELECT COUNT(*) FROM ...
then this function returns that count.

If your query is:
SELECT MAX(extract_date) FROM ...
then this function returns that date.

Why this is useful
A lot of your checks only need one number or one value back.
So instead of repeating this logic every time, you wrap it in one helper function.
"""
def run_scalar_query(client: bigquery.Client, query: str) -> Any:
    job = client.query(query)
    rows = list(job.result())
    if not rows:
        return None
    row = rows[0]
    return row[0]

"""
What it does:
Tries to convert a value into a float safely.

Why this matters:
BigQuery might return:
	•	integers
	•	decimals
	•	None

This function makes sure Python can work with the value as a number without crashing.
"""
def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

"""
What it does:
Creates a CheckResult object in a clean, reusable way.

Why it exists
Every check returns a result with the same basic shape:
	•	run id
	•	check name
	•	table
	•	status
	•	details
	•	etc.

This helper saves you from rewriting that object-building logic over and over.
"""
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

"""
What this check is doing:
It checks whether the latest extract_date in bronze orders equals today’s date.
"""
def evaluate_freshness_check(
    *,
    run_id: str,
    run_timestamp: datetime,
    client: bigquery.Client,
) -> CheckResult:
    query = f"""
    SELECT DATE(MAX(extract_date))
    FROM `{PROJECT_ID}.{BRONZE_DATASET}.shopify_orders_raw`
    """
    latest_extract_date = run_scalar_query(client, query)

    today_utc = utc_now().date()
    status = "pass" if latest_extract_date == today_utc else "fail"
    severity = "high" if status == "fail" else "low"
    details = (
        f"Latest orders extract_date is {latest_extract_date}; expected {today_utc}."
    )

    return build_result(
        run_id=run_id,
        run_timestamp=run_timestamp,
        check_name="bronze_orders_freshness_check",
        layer_name="bronze",
        table_name="shopify_orders_raw",
        metric_name="latest_extract_date_matches_today",
        metric_value=1.0 if status == "pass" else 0.0,
        threshold_value="extract_date must equal today (UTC)",
        status=status,
        severity=severity,
        details=details,
    )

"""
What this check is doing:
It counts how many rows exist for the latest extract date in a given table.

Works for:
	•	orders raw
	•	customers raw
	•	products raw
"""
def evaluate_latest_extract_row_count_check(
    *,
    run_id: str,
    run_timestamp: datetime,
    client: bigquery.Client,
    dataset: str,
    table_name: str,
    layer_name: str,
    check_name: str,
) -> CheckResult:
    query = f"""
    SELECT COUNT(*)
    FROM `{PROJECT_ID}.{dataset}.{table_name}`
    WHERE DATE(extract_date) = (
      SELECT DATE(MAX(extract_date))
      FROM `{PROJECT_ID}.{dataset}.{table_name}`
    )
    """
    latest_row_count = safe_float(run_scalar_query(client, query))

    if latest_row_count is None:
        status = "fail"
        severity = "high"
        details = f"Could not determine latest extract_date row count for {table_name}."
    elif latest_row_count == 0:
        status = "fail"
        severity = "high"
        details = f"Latest extract_date row count for {table_name} is 0."
    elif latest_row_count < 5:
        status = "warn"
        severity = "medium"
        details = f"Latest extract_date row count for {table_name} is very low: {latest_row_count:.0f}."
    else:
        status = "pass"
        severity = "low"
        details = f"Latest extract_date row count for {table_name} is {latest_row_count:.0f}."

    return build_result(
        run_id=run_id,
        run_timestamp=run_timestamp,
        check_name=check_name,
        layer_name=layer_name,
        table_name=table_name,
        metric_name="latest_extract_date_row_count",
        metric_value=latest_row_count,
        threshold_value="> 0 rows required; < 5 rows warns",
        status=status,
        severity=severity,
        details=details,
    )

"""
What this check is doing:
Looks for duplicate IDs.

Example:
	•	duplicate order_id
	•	duplicate customer_id
"""
def evaluate_duplicate_check(
    *,
    run_id: str,
    run_timestamp: datetime,
    client: bigquery.Client,
    dataset: str,
    table_name: str,
    id_column: str,
    layer_name: str,
    check_name: str,
) -> CheckResult:
    query = f"""
    SELECT COUNT(*)
    FROM (
      SELECT {id_column}
      FROM `{PROJECT_ID}.{dataset}.{table_name}`
      GROUP BY {id_column}
      HAVING COUNT(*) > 1
    )
    """
    duplicate_count = safe_float(run_scalar_query(client, query))

    if duplicate_count is None:
        status = "fail"
        severity = "high"
        details = f"Could not determine duplicate count for {table_name}.{id_column}."
    elif duplicate_count > 0:
        status = "fail"
        severity = "high"
        details = f"Found {duplicate_count:.0f} duplicated values in {table_name}.{id_column}."
    else:
        status = "pass"
        severity = "low"
        details = f"No duplicated values found in {table_name}.{id_column}."

    return build_result(
        run_id=run_id,
        run_timestamp=run_timestamp,
        check_name=check_name,
        layer_name=layer_name,
        table_name=table_name,
        metric_name=f"duplicate_{id_column}_count",
        metric_value=duplicate_count,
        threshold_value="0 duplicates allowed",
        status=status,
        severity=severity,
        details=details,
    )

"""
What this check is doing

Compares the latest days revenue in gold_daily_sales against the trailing average of prior days.

Big idea
It asks:
Is todays revenue abnormally low or high compared with recent history?

"""
def evaluate_gold_daily_sales_anomaly(
    *,
    run_id: str,
    run_timestamp: datetime,
    client: bigquery.Client,
) -> CheckResult:
    query = f"""
    WITH ordered_days AS (
      SELECT
        order_date,
        total_revenue
      FROM `{PROJECT_ID}.{GOLD_DATASET}.gold_daily_sales`
    ),
    latest_day AS (
      SELECT
        order_date,
        total_revenue
      FROM ordered_days
      QUALIFY order_date = MAX(order_date) OVER ()
    ),
    prior_days AS (
      SELECT total_revenue
      FROM ordered_days
      WHERE order_date < (SELECT order_date FROM latest_day)
      ORDER BY order_date DESC
      LIMIT 7
    )
    SELECT
      (SELECT total_revenue FROM latest_day) AS latest_revenue,
      (SELECT AVG(total_revenue) FROM prior_days) AS trailing_avg
    """
    job = client.query(query)
    rows = list(job.result())

    latest_revenue = None
    trailing_avg = None
    if rows:
      latest_revenue = safe_float(rows[0]["latest_revenue"])
      trailing_avg = safe_float(rows[0]["trailing_avg"])

    if latest_revenue is None:
        status = "fail"
        severity = "high"
        details = "Could not determine latest revenue from gold_daily_sales."
        metric_value = None
    elif trailing_avg is None or trailing_avg == 0:
        status = "pass"
        severity = "low"
        details = (
            f"Latest revenue is {latest_revenue:.2f}. Not enough prior data to evaluate anomaly baseline."
        )
        metric_value = latest_revenue
    else:
        pct_change = ((latest_revenue - trailing_avg) / trailing_avg) * 100.0
        metric_value = pct_change

        if pct_change <= -50:
            status = "fail"
            severity = "high"
        elif pct_change <= -30 or pct_change >= 50:
            status = "warn"
            severity = "medium"
        else:
            status = "pass"
            severity = "low"

        details = (
            f"Latest revenue is {latest_revenue:.2f}; trailing 7-day average is {trailing_avg:.2f}; "
            f"change is {pct_change:.2f}%."
        )

    return build_result(
        run_id=run_id,
        run_timestamp=run_timestamp,
        check_name="gold_daily_sales_anomaly_check",
        layer_name="gold",
        table_name="gold_daily_sales",
        metric_name="pct_change_vs_trailing_7d_avg",
        metric_value=metric_value,
        threshold_value="warn if <= -30% or >= 50%; fail if <= -50%",
        status=status,
        severity=severity,
        details=details,
    )

"""
What it does:
Takes all the CheckResult objects and inserts them into your monitoring table.
"""
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
        raise RuntimeError(f"Failed to insert monitoring results: {errors}")


def main() -> None:
    client = get_bq_client()

    # Airflow can pass AIRFLOW_CTX_DAG_RUN_ID automatically.
    run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID", f"manual_quality_run_{utc_now().isoformat()}")
    run_timestamp = utc_now()

    results: list[CheckResult] = []

    results.append(
        evaluate_freshness_check(
            run_id=run_id,
            run_timestamp=run_timestamp,
            client=client,
        )
    )

    results.append(
        evaluate_latest_extract_row_count_check(
            run_id=run_id,
            run_timestamp=run_timestamp,
            client=client,
            dataset=BRONZE_DATASET,
            table_name="shopify_orders_raw",
            layer_name="bronze",
            check_name="bronze_orders_row_count_check",
        )
    )

    results.append(
        evaluate_latest_extract_row_count_check(
            run_id=run_id,
            run_timestamp=run_timestamp,
            client=client,
            dataset=BRONZE_DATASET,
            table_name="shopify_customers_raw",
            layer_name="bronze",
            check_name="bronze_customers_row_count_check",
        )
    )

    results.append(
        evaluate_latest_extract_row_count_check(
            run_id=run_id,
            run_timestamp=run_timestamp,
            client=client,
            dataset=BRONZE_DATASET,
            table_name="shopify_products_raw",
            layer_name="bronze",
            check_name="bronze_products_row_count_check",
        )
    )

    results.append(
        evaluate_duplicate_check(
            run_id=run_id,
            run_timestamp=run_timestamp,
            client=client,
            dataset=BRONZE_DATASET,
            table_name="shopify_orders_raw",
            id_column="order_id",
            layer_name="bronze",
            check_name="duplicate_order_id_check",
        )
    )

    results.append(
        evaluate_duplicate_check(
            run_id=run_id,
            run_timestamp=run_timestamp,
            client=client,
            dataset=BRONZE_DATASET,
            table_name="shopify_customers_raw",
            id_column="customer_id",
            layer_name="bronze",
            check_name="duplicate_customer_id_check",
        )
    )

    results.append(
        evaluate_gold_daily_sales_anomaly(
            run_id=run_id,
            run_timestamp=run_timestamp,
            client=client,
        )
    )

    insert_results(client, results)

    print("Data quality checks completed successfully.")
    for result in results:
        print(
            f"{result.check_name}: status={result.status}, "
            f"metric_value={result.metric_value}, details={result.details}"
        )


if __name__ == "__main__":
    main()