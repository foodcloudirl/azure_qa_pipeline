import os
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, MetaData, text
import os
import logging

logger = logging.getLogger(__name__)


# only for local testing
if os.path.exists("local.settings.json"):
    with open("local.settings.json") as f:
        settings = json.load(f).get("Values", {})
        for k, v in settings.items():
            os.environ[k] = v




POSTGRES_ENGINE = os.environ.get("POSTGRES_ENGINE")
LDW_CONN_STRING = os.environ.get("LDW_CONN_STRING")


if not POSTGRES_ENGINE or not LDW_CONN_STRING:
    raise ValueError("Missing DB connection strings in environment variables.")


def get_edw_impact(conn, start_date, end_date, foodbank_name):
    """
    Constructs and executes the SQL query to retrieve the food redistribution impact data.

    Args:
    - conn (pyodbc.Connection): The database connection object.
    - start_date (str): The start date for the data range.
    - end_date (str): The end date for the data range.

    Returns:
    - pd.DataFrame: A DataFrame containing the query results.
    """
    query = f"""
    SELECT
        dimDonors.[Foodiverse official_id] AS official_id,
        dimDonors.[Foodiverse name] AS branch_name,
        SUM(CAST(factsFvEdw.total_actual_kgs AS FLOAT)) AS food_redistributed_actual,
        SUM(CAST(factsFvEdw.total_weight AS FLOAT)) AS food_redistributed_effective,
        SUM(CAST(factsFvEdw.total_estimated_kgs AS FLOAT)) AS food_redistributed_estimated
    FROM factsFvEdw
        JOIN dimDonors ON factsFvEdw.donor_id = dimDonors.[index]
        JOIN dimNetworks ON CAST(factsFvEdw.networks_id as varchar) = dimNetworks.network_id    
    WHERE factsFvEdw.Date BETWEEN '{start_date}' AND '{end_date}'
        AND factsFvEdw.donation_end_state = 'Ended'
        AND factsFvEdw.[Transaction Type] = 'Collected'
        AND dimNetworks.[Name] = '{foodbank_name}'
        AND dimDonors."Foodiverse tsm_current_state" != 'Removed'

    GROUP BY dimDonors.[Foodiverse official_id], dimDonors.[Foodiverse name]
    ORDER BY dimDonors.[Foodiverse official_id];
    """
    logger.info(query)
    print(query)
    result = pd.read_sql(query, conn)
    return result



def get_edw_kpis(conn, start_date, end_date, foodbank_name):
    """
    Constructs and executes the SQL query to retrieve KPI data from EDW.
    
    Args:
    - conn: The database connection object.
    - start_date (str): The start date for the data range.
    - end_date (str): The end date for the data range.
    - foodbank_name (str): The name of the food bank.
    
    Returns:
    - pd.DataFrame: A DataFrame containing the query results.
    """
    query = f"""
    SELECT 
        d."Foodiverse official_id" AS official_id,
        d."Foodiverse name" AS branch_name,
        d."Foodiverse parent_name" AS name,

        COUNT(DISTINCT CASE WHEN f.donation_state <> 'No Posting' THEN f.donation_id END) AS total_posted_count,
        COUNT(DISTINCT f.donation_response_id) AS total_offers_count,
        COUNT(DISTINCT f.donation_uuid) AS total_food_offered,

        COUNT(DISTINCT CASE WHEN f.transfer_success = 1 THEN f.donation_response_id END) AS total_transferred,
        COUNT(DISTINCT CASE WHEN f.accept_success = 1 THEN f.donation_response_id END) AS total_accepted

    FROM factsDonationOffersSummaryEdw f  
        JOIN dimDonors d ON f.donor_id = d."index"
        JOIN dimNetworks n ON CAST(f.networks_id AS VARCHAR) = n.network_id

    WHERE f.date BETWEEN '{start_date}' AND '{end_date}'
        AND d."Foodiverse name" IS NOT NULL
        AND n."Name" = '{foodbank_name}'
        AND d."Foodiverse tsm_current_state" != 'Removed'

    GROUP BY d."Foodiverse official_id", d."Foodiverse name", d."Foodiverse parent_name"
    ORDER BY d."Foodiverse official_id";
    """

    print("Executing SQL Query:")
    logger.info(query)
    print(query)

    df = pd.read_sql(query, conn)
    return df





def run_edw_qa_pipeline(start_date, end_date, report_type, foodbank_name):
    
    # Connect to EDW database
    try:
        engine = create_engine(LDW_CONN_STRING, echo=False, connect_args={'autocommit': True})
        print('Connected to the EDW database')
    except Exception as e:
        print('Connection failed')
        print(e)
        return None

    with engine.connect() as conn:    
        if report_type == 'impact':
            data = get_edw_impact(conn, start_date, end_date, foodbank_name)
        elif report_type == 'kpi':
            data = get_edw_kpis(conn, start_date, end_date, foodbank_name)
        else:
            print('Invalid report type')
            data = None
            raise ValueError('Invalid report type')
    
    return data




