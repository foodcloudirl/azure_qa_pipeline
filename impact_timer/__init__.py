
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

from qa_pipeline import Qa_Pipeline  # -- This brings in the full pipeline logic

import sys
sys.stdout.flush()
import os
import time
# -- Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -- Load DB connection strings


POSTGRES_ENGINE = os.environ.get("POSTGRES_ENGINE")
LDW_CONN_STRING = os.environ.get("LDW_CONN_STRING")

if not POSTGRES_ENGINE or not LDW_CONN_STRING:
    raise ValueError("Missing DB connection strings in environment variables.")


def determine_last_month():
    """
    -- This function calculates the last full calendar month to QA.
    -- e.g., if today is July 9, it returns (2025, 6).
    """
    today = datetime.utcnow()
    last_month = today - relativedelta(months=1)
    return last_month.year, last_month.month






def run_impact_qa_for_all_foodbanks(year, month):
    foodbanks = ['Slovakia Food Net', 'Czech Republic Food Net', 'FoodCloud', 'FareShare UK']
    print("Starting QA for all foodbanks...")

    for fb in foodbanks:
        try:
            print(f"\nInstantiating pipeline for {fb}..."); sys.stdout.flush()
            logger.info(f"[START] Impact QA for {fb} ({year}-{month:02d})")
            start_time = time.time()

            pipeline = Qa_Pipeline(
                year=year,
                period=month,
                period_type='month',
                report_type='impact',
                foodbank_name=fb
            )

            print(f" Running QA pipeline for {fb}"); sys.stdout.flush()
            pipeline.run_qa_pipeline()

            elapsed = round(time.time() - start_time, 2)
            logger.info(f"[SUCCESS] {fb} ({year}-{month:02d}) completed in {elapsed} seconds")
            print(f" Success: {fb} | Time: {elapsed}s")
        except Exception as e:
            logger.error(f" [FAIL] {fb}: {e}")
            print(f" Failed for {fb}: {e}")




def main(mytimer=None):
    """
    -- This is the Azure Function entry point.
    -- It ensures QA runs only once per month (for last completed month).
    """
    print(">> QA pipeline triggered manually <<")
    logger.info(f"Azure trigger fired at {datetime.utcnow().isoformat()}")

    # Step 1: Determine which period we want to run
    year, month = determine_last_month()
    logger.info(f" Target QA Month: {year}-{month:02d}")


    # Step 3: Run full impact QA pipeline
    run_impact_qa_for_all_foodbanks(year, month)


#if __name__ == "__main__":
 #   try:
 #       main()
 #   except Exception as e:
 #       print(">>> Error during manual QA run:", e)

