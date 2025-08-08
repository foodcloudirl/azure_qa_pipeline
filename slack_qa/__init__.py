import logging
import datetime
import traceback
import requests
import pandas as pd
from sqlalchemy import create_engine, text
import azure.functions as func
import os

# Load environment variables
POSTGRES_ENGINE = os.environ.get("POSTGRES_ENGINE")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")

if not POSTGRES_ENGINE or not SLACK_WEBHOOK_URL:
    raise ValueError("Missing POSTGRES_ENGINE or SLACK_WEBHOOK_URL env vars")

def get_previous_month():
    """
    Always returns the year and month of the previous full calendar month.
    """
    today = datetime.date.today()
    first_this_month = today.replace(day=1)
    last_month = first_this_month - datetime.timedelta(days=1)
    return last_month.year, last_month.month

def get_latest_run_timestamp(conn, table_name, year, month):
    """
    Get the latest run_timestamp timestamp for a given QA table, year, and month.
    """
    query = f"""
        SELECT MAX(run_timestamp) as last_run
        FROM {table_name}
        WHERE year = {year} AND period = {month}
    """
    result = conn.execute(text(query)).fetchone()
    return result[0] if result else None

def query_impact_summary(conn, year, month, last_run):
    """
    Query impact summary data for only the latest run timestamp.
    """
    query = f"""
        SELECT
          organisation_name,
          SUM(actuals_difference) AS diff_actual,
          SUM(effective_difference) AS diff_effective,
          SUM(estimated_difference) AS diff_estimated,
          SUM(food_redistributed_effective_api) AS api_effective,
          SUM(food_redistributed_actual_api) AS api_actual,
          SUM(food_redistributed_estimated_api) AS api_estimated
        FROM public.impact_qa_monthly_v2
        WHERE year = {year} AND period = {month}
          AND run_timestamp = '{last_run}'
        GROUP BY organisation_name
    """
    return pd.read_sql(query, conn)

def query_kpi_summary(conn, year, month, last_run):
    """
    Query KPI summary data for only the latest run timestamp.
    """
    query = f"""
        SELECT
          organisation_name,
          SUM(total_food_offered_difference) AS diff_offered,
          SUM(total_transferred_difference) AS diff_transferred,
          SUM(total_accepted_difference) AS diff_accepted,
          SUM(total_food_offered_edw) AS offered_edw,
          SUM(total_food_offered_api) AS offered_api,
          SUM(total_transferred_edw) AS transferred_edw,
          SUM(total_transferred_api) AS transferred_api,
          SUM(total_accepted_edw) AS accepted_edw,
          SUM(total_accepted_api) AS accepted_api
        FROM public.kpi_qa_monthly
        WHERE year = {year} AND period = {month}
          AND run_timestamp = '{last_run}'
        GROUP BY organisation_name
    """
    return pd.read_sql(query, conn)

def fetch_data(year, month):
    """
    Fetch impact and KPI data from the latest QA run within the target month.
    """
    engine = create_engine(f"{POSTGRES_ENGINE}/postgres", isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        latest_impact_run = get_latest_run_timestamp(conn, 'public.impact_qa_monthly_v2', year, month)
        latest_kpi_run = get_latest_run_timestamp(conn, 'public.kpi_qa_monthly', year, month)

        if not latest_impact_run or not latest_kpi_run:
            raise Exception("No recent QA run found for the given month")

        impact_df = query_impact_summary(conn, year, month, latest_impact_run)
        kpi_df = query_kpi_summary(conn, year, month, latest_kpi_run)

    return impact_df, kpi_df

def format_slack_message(impact_df, kpi_df, year, month):
    """
    Creates a formatted plain-text message summarizing QA results for Slack.
    """
    msg = [
        f"QA Summary for {year}-{month:02d}",
        f"Run at: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        ""
    ]

    tesco_orgs = {"Tesco UK", "Tesco SK", "Tesco IE", "Tesco CZ"}
    combined = impact_df.merge(kpi_df, on="organisation_name", how="outer").fillna(0)

    for _, row in combined.iterrows():
        name = row['organisation_name']
        msg.append(f"Organisation: {name}")

        # Impact formatting
        if name in tesco_orgs:
            impact_pass = abs(row['diff_actual']) < 1e-4
            impact_value = row['api_actual']
            impact_type = "Actual"
        else:
            impact_pass = abs(row['diff_effective']) < 1e-4
            impact_value = row['api_effective']
            impact_type = "Effective"

        impact_status = "PASS" if impact_pass else f"FAIL ({impact_type}: {impact_value:,.1f})"
        msg.append(f"Impact: {impact_status}")

        # KPI formatting
        kpi_pass = all(abs(row[col]) < 1e-4 for col in ['diff_offered', 'diff_transferred', 'diff_accepted'])
        if kpi_pass:
            msg.append("KPI: PASS")
        else:
            msg.append("KPI (EDW vs API vs Diff):")
            msg.append(f"- Offered: {row['offered_edw']:.1f} vs {row['offered_api']:.1f} vs {abs(row['diff_offered']):.1f}")
            msg.append(f"- Transferred: {row['transferred_edw']:.1f} vs {row['transferred_api']:.1f} vs {abs(row['diff_transferred']):.1f}")
            msg.append(f"- Accepted: {row['accepted_edw']:.1f} vs {row['accepted_api']:.1f} vs {abs(row['diff_accepted']):.1f}")

        msg.append("")

    msg.append("View Full QA Dashboard: https://app.powerbi.com/groups/77e58145-ca8b-4fd4-96ea-f20327008da2/reports/62a290ef-5c0f-40d7-9bb4-04385dfd1c09/09b2bdb0b36550b3adbe?experience=power-bi&clientSideAuth=0")
    return "\n".join(msg)

def send_slack_message(text):
    """
    Sends a plain text message to the Slack webhook defined in env vars.
    """
    try:
        res = requests.post(SLACK_WEBHOOK_URL, json={"text": text})
        res.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Slack message failed: {e}")
        raise

def main(mytimer: func.TimerRequest) -> None:
    """
    Main Azure Function entrypoint. Gathers last month's QA data and sends to Slack.
    """
    logging.info("Slack QA Function Triggered")
    try:
        year, month = get_previous_month()
        impact_df, kpi_df = fetch_data(year, month)
        message = format_slack_message(impact_df, kpi_df, year, month)
        send_slack_message(message)
        logging.info("Slack notification sent successfully")
    except Exception as e:
        error_msg = f"Slack QA Notification Failed\nError: {str(e)}"
        logging.error(error_msg)
        send_slack_message(error_msg)