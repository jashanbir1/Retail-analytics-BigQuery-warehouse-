from __future__ import annotations

import os
from pathlib import Path

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
    page_title="Retail Data Quality Monitor",
    page_icon="📊",
    layout="wide",
)

st.title("Retail Data Quality Monitor")
st.caption("Warehouse health checks, anomaly detection, and AI explanations")

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

issues_df = run_df[run_df["status"].isin(["warn", "fail"])].copy()

st.subheader("Warnings and Failures")

if issues_df.empty:
    st.success("No warnings or failures in this run.")
else:
    for _, row in issues_df.iterrows():
        header = f"{row['check_name']} | {row['status'].upper()} | {row['layer_name']}.{row['table_name']}"
        if row["status"] == "fail":
            box = st.expander(header, expanded=True)
        else:
            box = st.expander(header, expanded=False)

        with box:
            c1, c2, c3 = st.columns(3)
            c1.write(f"**Metric**: {row['metric_name']}")
            c2.write(f"**Metric Value**: {row['metric_value']}")
            c3.write(f"**Threshold**: {row['threshold_value']}")

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