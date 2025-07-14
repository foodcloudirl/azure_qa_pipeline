
"""
-- This script runs automated monthly Impact QA using the Qa_Pipeline class.
-- It checks the last complete calendar month, skips if already run (via database check),
-- and runs full API vs EDW QA pipeline for all foodbanks.
-- Intended to run as an Azure Function (e.g., on the 7th of each month).
"""

import logging
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text

from auto_impact_qa.qa_pipeline import Qa_Pipeline  # -- This brings in the full pipeline logic

# -- Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -- Load DB connection strings
try:
    with open("connection_strings.json") as f:
        conn_data = json.load(f)
        POSTGRES_ENGINE = conn_data.get("engine")
        if not POSTGRES_ENGINE:
            raise ValueError("Missing 'engine' key in connection_strings.json")
except Exception as e:
    raise RuntimeError(f"Connection string error: {e}")



def determine_last_month():
    """
    -- This function calculates the last full calendar month to QA.
    -- e.g., if today is July 9, it returns (2025, 6).
    """
    today = datetime.utcnow()
    last_month = today - relativedelta(months=1)
    return last_month.year, last_month.month



def run_impact_qa_for_all_foodbanks(year, month):
    """
    -- This function instantiates and runs the Qa_Pipeline for each foodbank for the given period.
    -- It fully executes the impact QA logic using the existing pipeline.
    """
    foodbanks = ['Slovakia Food Net', 'Czech Republic Food Net', 'FoodCloud', 'FareShare UK']

    for fb in foodbanks:
        try:
            logger.info(f" Running Impact QA for {fb} ({year}-{month:02d})")
            pipeline = Qa_Pipeline(
                year=year,
                period=month,
                period_type='month',
                report_type='impact',
                foodbank_name=fb
            )
            pipeline.run_qa_pipeline()
            logger.info(f"Success: {fb} ({year}-{month:02d})")
        except Exception as e:
            logger.error(f" Failed for {fb}: {e}")



def main(mytimer=None):
    """
    -- This is the Azure Function entry point.
    -- It ensures QA runs only once per month (for last completed month).
    """
    logger.info(f"Azure trigger fired at {datetime.utcnow().isoformat()}")

    # Step 1: Determine which period we want to run
    year, month = determine_last_month()
    logger.info(f" Target QA Month: {year}-{month:02d}")


    # Step 3: Run full impact QA pipeline
    run_impact_qa_for_all_foodbanks(year, month)
