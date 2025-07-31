"""
-- This script runs automated monthly KPI QA using the Qa_Pipeline class.
-- It checks the last complete calendar month, skips if already run (via database check),
-- and runs full API vs EDW QA pipeline for all foodbanks.
-- Intended to run as an Azure Function (e.g., every week on Thursday).
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






def run_KPI_qa_for_all_foodbanks(year, month):
    """
    Run the full KPI QA pipeline for each foodbank for the specified year and month.

    This function initializes and executes the QA pipeline for a list of foodbanks, ensuring
    each is only processed once per run. It logs the start and completion (or failure) of each
    QA job and provides a final log when all are complete.

    Args:
        year (int): The target year for the QA run.
        month (int): The target month for the QA run.
    """
    foodbanks = ['Slovakia Food Net', 'Czech Republic Food Net', 'FoodCloud', 'FareShare UK']
    logger.info("Starting QA for all foodbanks...")

    executed = set()  #  Track which foodbanks have been processed

    for fb in foodbanks:
        if fb in executed:
            logger.warning(f"[SKIP] {fb} already processed in this run.")
            continue
        executed.add(fb)  #  Mark as processed

        try:
            logger.info(f"\n[START] KPI QA for {fb} ({year}-{month:02d})")
            start_time = time.time()

            pipeline = Qa_Pipeline(
                year=year,
                period=month,
                period_type='month',
                report_type='kpi',
                foodbank_name=fb
            )

            pipeline.run_kpi_pipeline()

            elapsed = round(time.time() - start_time, 2)
            logger.info(f"[SUCCESS] {fb} ({year}-{month:02d}) completed in {elapsed} seconds")
        except Exception as e:
            logger.error(f"[FAIL] {fb}: {e}")

    logger.info("[DONE] QA run complete for all foodbanks.")



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


    # Step 3: Run full KPI QA pipeline
    run_KPI_qa_for_all_foodbanks(year, month)


#if __name__ == "__main__":
 #   try:
  #      main()
   # except Exception as e:
    #   print(">>> Error during manual QA run:", e)
