from __future__ import annotations

import os
from pathlib import Path

import anthropic
import pandas as pd
import streamlit as st
from google.cloud import bigquery


PROJECT_ID = "retail-data-warehouse-project"
MONITORING_DATASET = "retail_monitoring"
MONITORING_TABLE = "data_quality_results"


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

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

_WAREHOUSE_SCHEMA = """
You are a data analyst assistant for a Shopify retail data warehouse on Google BigQuery
(project: retail-data-warehouse-project).

Always fully qualify table names with backticks:
  `retail-data-warehouse-project.retail_gold.gold_daily_sales`

Limit results to 100 rows unless the user asks for more.
All revenue values are floats in the store's currency (typically USD).

=== AVAILABLE TABLES ===

retail_gold.gold_daily_sales
  order_date        DATE      – calendar date of orders
  total_orders      INTEGER   – distinct orders placed
  total_units_sold  INTEGER   – total quantity of items sold
  total_revenue     FLOAT     – sum of line revenues

retail_gold.gold_customer_lifetime_value
  customer_id           STRING
  customer_full_name    STRING
  email                 STRING
  phone_number          STRING
  currency              STRING
  total_orders          INTEGER
  total_units_purchased INTEGER
  lifetime_revenue      FLOAT
  average_order_value   FLOAT

retail_gold.gold_product_sales
  product_id            STRING
  product_title         STRING
  vendor                STRING
  total_orders          INTEGER
  total_units_sold      INTEGER
  all_time_revenue      FLOAT
  avg_revenue_per_order FLOAT
  avg_units_per_order   FLOAT

retail_gold.gold_order_basket_behavior  (one row per order)
  order_id              STRING
  customer_id           STRING
  order_date            DATE
  line_item_count       INTEGER  – distinct products in the order
  total_units_in_order  INTEGER
  order_revenue         FLOAT
  fulfillment_status    STRING   – 'fulfilled', 'unfulfilled', etc.
  is_fulfilled          BOOLEAN

retail_gold.fact_order_line_items  (one row per line item)
  line_item_id       STRING
  order_id           STRING
  customer_id        STRING
  product_id         STRING
  variant_id         STRING
  order_date         DATE
  quantity           INTEGER
  unit_price         FLOAT
  line_revenue       FLOAT    – quantity * unit_price
  fulfillment_status STRING
  requires_shipping  BOOLEAN
  taxable            BOOLEAN

retail_gold.dim_customers
  customer_id          STRING
  first_name           STRING
  last_name            STRING
  customer_full_name   STRING
  email                STRING
  phone_number         STRING
  customer_state       STRING
  verified_email       BOOLEAN
  tax_exempt           BOOLEAN
  tags                 STRING
  currency             STRING
  customer_created_at  TIMESTAMP
  customer_updated_at  TIMESTAMP

retail_gold.dim_products
  product_id    STRING
  product_title STRING
  vendor        STRING

retail_monitoring.data_quality_results
  run_id           STRING
  run_timestamp    TIMESTAMP
  check_name       STRING
  layer_name       STRING   – 'bronze' or 'gold'
  table_name       STRING
  metric_name      STRING
  metric_value     FLOAT
  threshold_value  STRING
  status           STRING   – 'pass', 'warn', 'fail'
  severity         STRING   – 'low', 'medium', 'high'
  details          STRING
  ai_explanation   STRING
  likely_causes    STRING
  suggested_actions STRING
""".strip()

_RUN_SQL_TOOL = {
    "name": "run_bigquery_sql",
    "description": (
        "Execute a SQL query against the retail BigQuery warehouse and return results. "
        "Use this whenever you need data to answer the user's question."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "A valid BigQuery SQL query with fully-qualified, backtick-quoted table names.",
            }
        },
        "required": ["sql"],
    },
}


def _run_nl_sql_agent(
    question: str, bq_client: bigquery.Client
) -> tuple[str, str | None, pd.DataFrame | None]:
    """Run the observe-reason-act loop: Claude generates SQL, we execute it, Claude answers."""
    if not ANTHROPIC_API_KEY or ANTHROPIC_API_KEY == "your-anthropic-api-key-here":
        return (
            "Set ANTHROPIC_API_KEY in your .env file to enable the chat feature.",
            None,
            None,
        )

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    messages: list[dict] = [{"role": "user", "content": question}]
    generated_sql: str | None = None
    result_df: pd.DataFrame | None = None

    while True:
        response = client.messages.create(
            model="claude-opus-4-7",
            max_tokens=4096,
            system=[
                {
                    "type": "text",
                    "text": _WAREHOUSE_SCHEMA,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            tools=[_RUN_SQL_TOOL],
            messages=messages,
        )

        if response.stop_reason == "end_turn":
            answer = next(
                (b.text for b in response.content if b.type == "text"), "Done."
            )
            return answer, generated_sql, result_df

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []

            for block in response.content:
                if block.type != "tool_use" or block.name != "run_bigquery_sql":
                    continue
                sql: str = block.input["sql"]
                generated_sql = sql
                try:
                    result_df = bq_client.query(sql).to_dataframe()
                    result_str = (
                        "Query returned no rows."
                        if result_df.empty
                        else result_df.head(100).to_string(index=False)
                    )
                except Exception as exc:
                    result_df = None
                    result_str = f"SQL Error: {exc}"
                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result_str,
                            "is_error": True,
                        }
                    )
                    continue
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result_str,
                    }
                )

            messages.append({"role": "user", "content": tool_results})
        else:
            answer = next(
                (b.text for b in response.content if b.type == "text"),
                "Unexpected response.",
            )
            return answer, generated_sql, result_df


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


@st.cache_data(ttl=60)
def run_query(query: str) -> pd.DataFrame:
    client = get_bq_client()
    return client.query(query).to_dataframe()


def get_latest_run_id() -> str | None:
    query = f"""
    SELECT run_id
    FROM `{PROJECT_ID}.{MONITORING_DATASET}.{MONITORING_TABLE}`
    ORDER BY run_timestamp DESC
    LIMIT 1
    """
    df = run_query(query)
    if df.empty:
        return None
    return str(df.iloc[0]["run_id"])


def get_recent_runs(limit: int = 20) -> pd.DataFrame:
    query = f"""
    SELECT
      run_id,
      run_timestamp,
      COUNT(*) AS total_checks,
      SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) AS pass_count,
      SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) AS warn_count,
      SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) AS fail_count
    FROM `{PROJECT_ID}.{MONITORING_DATASET}.{MONITORING_TABLE}`
    GROUP BY run_id, run_timestamp
    ORDER BY run_timestamp DESC
    LIMIT {limit}
    """
    return run_query(query)


def get_run_details(run_id: str) -> pd.DataFrame:
    query = f"""
    SELECT
      run_id,
      run_timestamp,
      check_timestamp,
      check_name,
      layer_name,
      table_name,
      metric_name,
      metric_value,
      threshold_value,
      status,
      severity,
      details,
      ai_prompt,
      ai_explanation,
      likely_causes,
      suggested_actions
    FROM `{PROJECT_ID}.{MONITORING_DATASET}.{MONITORING_TABLE}`
    WHERE run_id = @run_id
    ORDER BY
      CASE status
        WHEN 'fail' THEN 1
        WHEN 'warn' THEN 2
        WHEN 'pass' THEN 3
        ELSE 4
      END,
      check_name
    """
    client = get_bq_client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id)
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()


def get_status_counts(df: pd.DataFrame) -> tuple[int, int, int]:
    pass_count = int((df["status"] == "pass").sum()) if not df.empty else 0
    warn_count = int((df["status"] == "warn").sum()) if not df.empty else 0
    fail_count = int((df["status"] == "fail").sum()) if not df.empty else 0
    return pass_count, warn_count, fail_count


def format_run_label(row: pd.Series) -> str:
    ts = pd.to_datetime(row["run_timestamp"])
    return f"{ts.strftime('%Y-%m-%d %H:%M:%S')} | {row['run_id']}"


st.set_page_config(
    page_title="Retail Analytics Data Quality Monitor",
    page_icon="📊",
    layout="wide",
)

st.title("Retail Analytics Data Quality Monitor")
st.caption("Interactive site for Warehouse health checks, AI explanations, ")

recent_runs_df = get_recent_runs()

if recent_runs_df.empty:
    st.warning("No data quality results found yet in BigQuery.")
    st.stop()

run_options = {
    format_run_label(row): row["run_id"]
    for _, row in recent_runs_df.iterrows()
}

latest_run_id = get_latest_run_id()
default_index = 0

for i, (_, row) in enumerate(recent_runs_df.iterrows()):
    if row["run_id"] == latest_run_id:
        default_index = i
        break

selected_label = st.sidebar.selectbox(
    "Select pipeline run",
    list(run_options.keys()),
    index=default_index,
)

selected_run_id = run_options[selected_label]
run_df = get_run_details(selected_run_id)

if run_df.empty:
    st.warning("No check results found for the selected run.")
    st.stop()

run_timestamp = pd.to_datetime(run_df["run_timestamp"].iloc[0])

pass_count, warn_count, fail_count = get_status_counts(run_df)
total_checks = len(run_df)

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Run Timestamp", run_timestamp.strftime("%Y-%m-%d %H:%M:%S UTC"))
col2.metric("Total Checks", total_checks)
col3.metric("Pass", pass_count)
col4.metric("Warn", warn_count)
col5.metric("Fail", fail_count)

st.divider()

left, right = st.columns([1.3, 1])

with left:
    st.subheader("Latest Check Results")

    display_df = run_df[
        [
            "check_name",
            "layer_name",
            "table_name",
            "metric_name",
            "metric_value",
            "status",
            "severity",
            "details",
        ]
    ].copy()

    st.dataframe(display_df, use_container_width=True, hide_index=True)

with right:
    st.subheader("Run Summary")

    if fail_count > 0:
        st.error(f"This run has {fail_count} failed check(s).")
    elif warn_count > 0:
        st.warning(f"This run has {warn_count} warning check(s).")
    else:
        st.success("All checks passed for this run.")

    st.write("**Run ID**")
    st.code(selected_run_id)

    st.write("**Layers checked**")
    layers = sorted(run_df["layer_name"].dropna().unique().tolist())
    st.write(", ".join(layers) if layers else "N/A")

st.divider()

LAYER_BADGE = {
    "bronze": "🟤 Bronze",
    "silver": "⚪ Silver",
    "gold":   "🟡 Gold",
}

issues_df = run_df[run_df["status"].isin(["warn", "fail"])].copy()

st.subheader("Warnings and Failures")

if issues_df.empty:
    st.success("No warnings or failures in this run.")
else:
    for _, row in issues_df.iterrows():
        layer = row["layer_name"] or "unknown"
        badge = LAYER_BADGE.get(layer, layer)
        status_label = "🔴 FAIL" if row["status"] == "fail" else "🟠 WARN"
        header = f"{status_label} | {badge} | {row['check_name']}"

        with st.expander(header, expanded=row["status"] == "fail"):
            c1, c2, c3 = st.columns(3)
            c1.write(f"**Metric**: {row['metric_name']}")
            c2.write(f"**Metric Value**: {row['metric_value']}")
            c3.write(f"**Threshold**: {row['threshold_value']}")

            st.write(f"**Table**: `{row['layer_name']}.{row['table_name']}`")
            st.write(f"**Details**: {row['details']}")
            st.write(f"**AI Explanation**: {row['ai_explanation'] or 'N/A'}")
            st.write(f"**Likely Causes**: {row['likely_causes'] or 'N/A'}")
            st.write(f"**Suggested Actions**: {row['suggested_actions'] or 'N/A'}")

st.divider()

st.subheader("Recent Run History")

history_df = recent_runs_df.copy()
history_df["run_timestamp"] = pd.to_datetime(history_df["run_timestamp"])
history_df = history_df[
    ["run_timestamp", "run_id", "total_checks", "pass_count", "warn_count", "fail_count"]
]

st.dataframe(history_df, use_container_width=True, hide_index=True)

st.divider()

st.subheader("All Checks for Selected Run")

check_names = run_df["check_name"].tolist()
selected_check = st.selectbox("Inspect a check", check_names)

selected_check_row = run_df[run_df["check_name"] == selected_check].iloc[0]

detail_col1, detail_col2 = st.columns(2)

with detail_col1:
    st.write("**Check Metadata**")
    st.write(f"**Check Name**: {selected_check_row['check_name']}")
    st.write(f"**Layer**: {selected_check_row['layer_name']}")
    st.write(f"**Table**: {selected_check_row['table_name']}")
    st.write(f"**Metric**: {selected_check_row['metric_name']}")
    st.write(f"**Metric Value**: {selected_check_row['metric_value']}")
    st.write(f"**Threshold**: {selected_check_row['threshold_value']}")
    st.write(f"**Status**: {selected_check_row['status']}")
    st.write(f"**Severity**: {selected_check_row['severity']}")

with detail_col2:
    st.write("**Operational Notes**")
    st.write(f"**Details**: {selected_check_row['details']}")
    st.write(f"**AI Explanation**: {selected_check_row['ai_explanation'] or 'N/A'}")
    st.write(f"**Likely Causes**: {selected_check_row['likely_causes'] or 'N/A'}")
    st.write(f"**Suggested Actions**: {selected_check_row['suggested_actions'] or 'N/A'}")

with st.expander("Show AI Prompt"):
    st.code(selected_check_row["ai_prompt"] or "No AI prompt stored for this check.")

st.divider()

st.subheader("Ask Your Data")
st.caption(
    "Hi I am Enzo! Ask any question about your retail data in plain English. "
    "I will write and run the query for you."
)

if "chat_messages" not in st.session_state:
    st.session_state.chat_messages = []

for msg in st.session_state.chat_messages:
    with st.chat_message(msg["role"]):
        if msg["role"] == "assistant":
            if msg.get("sql"):
                with st.expander("Generated SQL", expanded=False):
                    st.code(msg["sql"], language="sql")
            if msg.get("df") is not None and not msg["df"].empty:
                st.dataframe(msg["df"], use_container_width=True, hide_index=True)
        st.write(msg["content"])

if question := st.chat_input("What would you like to know?"):
    st.session_state.chat_messages.append({"role": "user", "content": question})

    with st.chat_message("user"):
        st.write(question)

    with st.chat_message("assistant"):
        with st.spinner("Querying your warehouse..."):
            answer, sql, df = _run_nl_sql_agent(question, get_bq_client())

        if sql:
            with st.expander("Generated SQL", expanded=False):
                st.code(sql, language="sql")
        if df is not None and not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
        st.write(answer)

    st.session_state.chat_messages.append(
        {"role": "assistant", "content": answer, "sql": sql, "df": df}
    )