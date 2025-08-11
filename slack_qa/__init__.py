import logging
import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine, text
import azure.functions as func
import os

# Load environment variables from Azure Function environment
POSTGRES_ENGINE = os.environ.get("POSTGRES_ENGINE")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")

if not POSTGRES_ENGINE or not SLACK_WEBHOOK_URL:
    raise ValueError("Missing POSTGRES_ENGINE or SLACK_WEBHOOK_URL env vars")

def get_previous_month():
    """
    Get the year and month for the previous calendar month.
    Returns:
        tuple: (year, month) for the previous month.
    """
    today = datetime.date.today()
    first_this_month = today.replace(day=1)
    last_month = first_this_month - datetime.timedelta(days=1)
    return last_month.year, last_month.month

def query_impact_summary(conn, year, month):
    """
    Get the most recent QA run data for each organisation from the impact table.

    Args:
        conn (sqlalchemy.engine.Connection): Active DB connection.
        year (int): Target year.
        month (int): Target month (period).

    Returns:
        pandas.DataFrame: Aggregated impact QA data for each organisation.
    """
    query = f"""
        SELECT
          t.organisation_name,
          SUM(t.actuals_difference) AS diff_actual,
          SUM(t.effective_difference) AS diff_effective,
          SUM(t.estimated_difference) AS diff_estimated,
          SUM(t.food_redistributed_effective_api) AS api_effective,
          SUM(t.food_redistributed_actual_api) AS api_actual,
          SUM(t.food_redistributed_estimated_api) AS api_estimated
        FROM public.impact_qa_monthly_v2 t
        JOIN (
            SELECT organisation_name, MAX(run_timestamp) AS last_run
            FROM public.impact_qa_monthly_v2
            WHERE year = {year} AND period = {month}
            GROUP BY organisation_name
        ) latest
        ON t.organisation_name = latest.organisation_name
        AND t.run_timestamp = latest.last_run
        WHERE t.year = {year} AND t.period = {month}
        GROUP BY t.organisation_name
    """
    return pd.read_sql(query, conn)

def query_kpi_summary(conn, year, month):
    """
    Get the most recent QA run data for each organisation from the KPI table.

    Args:
        conn (sqlalchemy.engine.Connection): Active DB connection.
        year (int): Target year.
        month (int): Target month (period).

    Returns:
        pandas.DataFrame: Aggregated KPI QA data for each organisation.
    """
    query = f"""
        SELECT
          t.organisation_name,
          SUM(t.total_food_offered_difference) AS diff_offered,
          SUM(t.total_transferred_difference) AS diff_transferred,
          SUM(t.total_accepted_difference) AS diff_accepted,
          SUM(t.total_food_offered_edw) AS offered_edw,
          SUM(t.total_food_offered_api) AS offered_api,
          SUM(t.total_transferred_edw) AS transferred_edw,
          SUM(t.total_transferred_api) AS transferred_api,
          SUM(t.total_accepted_edw) AS accepted_edw,
          SUM(t.total_accepted_api) AS accepted_api
        FROM public.kpi_qa_monthly t
        JOIN (
            SELECT organisation_name, MAX(run_timestamp) AS last_run
            FROM public.kpi_qa_monthly
            WHERE year = {year} AND period = {month}
            GROUP BY organisation_name
        ) latest
        ON t.organisation_name = latest.organisation_name
        AND t.run_timestamp = latest.last_run
        WHERE t.year = {year} AND t.period = {month}
        GROUP BY t.organisation_name
    """
    return pd.read_sql(query, conn)

def fetch_data(year, month):
    """
    Fetch both impact and KPI data for the given year and month.

    Args:
        year (int): Target year.
        month (int): Target month (period).

    Returns:
        tuple: (impact_df, kpi_df) DataFrames containing QA data.
    """
    engine = create_engine(f"{POSTGRES_ENGINE}/postgres", isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        impact_df = query_impact_summary(conn, year, month)
        kpi_df = query_kpi_summary(conn, year, month)
    return impact_df, kpi_df

def format_slack_message(impact_df, kpi_df, year, month):
    """
    Format the QA results into a Slack-friendly message.

    Args:
        impact_df (pd.DataFrame): Impact QA data.
        kpi_df (pd.DataFrame): KPI QA data.
        year (int): Target year.
        month (int): Target month.

    Returns:
        str: Formatted message for Slack.
    """
    msg = [
        f"QA Summary for {year}-{month:02d}",
        f"Run at: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        ""
    ]

    # Clean organisation names to avoid merge mismatches
    impact_df['organisation_name'] = impact_df['organisation_name'].astype(str).str.strip()
    kpi_df['organisation_name'] = kpi_df['organisation_name'].astype(str).str.strip()

    tesco_orgs = {"Tesco UK", "Tesco SK", "Tesco IE", "Tesco CZ"}

    # Create a union of all organisations from both datasets
    all_orgs = pd.DataFrame({
        'organisation_name': pd.concat([
            impact_df['organisation_name'],
            kpi_df['organisation_name']
        ]).drop_duplicates()
    })

    # Merge to keep all orgs present even if missing from one dataset
    combined = all_orgs.merge(impact_df, on='organisation_name', how='left') \
                       .merge(kpi_df, on='organisation_name', how='left') \
                       .fillna(0)

    # Loop through each organisation to build message
    for _, row in combined.iterrows():
        name = row['organisation_name']

        # Impact section: Tesco gets Actual, others get Effective
        if name in tesco_orgs:
            impact_value = row['api_actual'] if row['api_actual'] > 0 else row['api_effective']
            impact_type = "Actual" if row['api_actual'] > 0 else "Actual (fallback to Effective)"
            diff_val = row['diff_actual'] if row['api_actual'] > 0 else row['diff_effective']
        else:
            impact_value = row['api_effective']
            impact_type = "Effective"
            diff_val = row['diff_effective']

        msg.append(f"Organisation: {name}")
        impact_status = "PASS" if abs(diff_val) < 1e-4 else "FAIL"
        msg.append(f"Impact: {impact_status} ({impact_type}: {impact_value:,.2f} kg)")

        # KPI section: Always show totals
        kpi_pass = all(abs(row[col]) < 1e-4 for col in ['diff_offered', 'diff_transferred', 'diff_accepted'])
        if kpi_pass:
            msg.append("KPI: PASS")
            msg.append(f"KPI Totals - Offered: {row['offered_edw']:.1f} vs {row['offered_api']:.1f} (Diff: {abs(row['diff_offered']):.1f})")
            msg.append(f"              Transferred: {row['transferred_edw']:.1f} vs {row['transferred_api']:.1f} (Diff: {abs(row['diff_transferred']):.1f})")
            msg.append(f"              Accepted: {row['accepted_edw']:.1f} vs {row['accepted_api']:.1f} (Diff: {abs(row['diff_accepted']):.1f})")
        else:
            msg.append("KPI (EDW vs API vs Diff):")
            msg.append(f"- Offered: {row['offered_edw']:.1f} vs {row['offered_api']:.1f} vs {abs(row['diff_offered']):.1f}")
            msg.append(f"- Transferred: {row['transferred_edw']:.1f} vs {row['transferred_api']:.1f} vs {abs(row['diff_transferred']):.1f}")
            msg.append(f"- Accepted: {row['accepted_edw']:.1f} vs {row['accepted_api']:.1f} vs {abs(row['diff_accepted']):.1f}")

        msg.append("")

    # Add grand total effective redistribution
    total_effective = impact_df['api_effective'].sum()
    msg.append(f"Total Effective Redistribution: {total_effective:,.2f} kg")
    msg.append("View Full QA Dashboard: https://app.powerbi.com/groups/77e58145-ca8b-4fd4-96ea-f20327008da2/reports/62a290ef-5c0f-40d7-9bb4-04385dfd1c09/09b2bdb0b36550b3adbe?experience=power-bi&clientSideAuth=0&bookmarkGuid=870307d46c90ecc537d7")
    return "\n".join(msg)

def send_slack_message(text):
    """
    Send a formatted message to Slack via webhook.
    """
    try:
        res = requests.post(SLACK_WEBHOOK_URL, json={"text": text})
        res.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Slack message failed: {e}")
        raise

def main(mytimer: func.TimerRequest) -> None:
    """
    Azure Function entry point.
    Triggered by timer to run monthly and send QA summary to Slack.
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
