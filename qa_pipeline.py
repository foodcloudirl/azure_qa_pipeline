'''
Module containing the QA Pipeline class to handle QA checks for Data Warehouse data against Foodiverse API data.
'''
import time 
import json
import logging
import uuid  # for generating unique run ID
from sqlalchemy import create_engine, text

import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests

from sqlalchemy import create_engine
import sys
sys.stdout.flush()

logger = logging.getLogger(__name__)

from api_qa import get_api_impact, get_api_kpis
from edw_qa import get_edw_impact, get_edw_kpis
import os


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

def connect_with_retry(connection_string, retries=5, delay=10):
    """
    Attempts to connect to the EDW with retry logic to handle Synapse cold-start delays.
    """
    for attempt in range(1, retries + 1):
        try:
            engine = create_engine(
                f"{connection_string}/ldw",
                echo=False,
                connect_args={'autocommit': True}
            )
            conn = engine.connect()
            print(f"[EDW] Connected on attempt {attempt}")
            return engine
        except Exception as e:
            print(f"[EDW] Connection attempt {attempt} failed: {e}")
            if attempt < retries:
                print(f"[EDW] Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                raise



class Qa_Pipeline:
    """
    Class to handle QA checks for Data Warehouse data against Foodiverse API data.
    An instance of QA Pipeline class is created to handle a given period type, period, year and report type.
    The class contains all variables and functions required to complete a qa run.
                        Get report from Foodiverse API
    Set up variables -> Get report from Data Warehouse -> Preprocess based on source -> Merge data -> Run QA checks -> Generate report -> Send to db / edw
                        Get report form any other source

    It is designed to be modular and self-contained.
    I.e., the class is instantiated with the required parameters. Functions to process the steps in the pipeline can be easily edited, added etc.
    It is easy to extend the class to include additional steps / sources / report types in the pipeline.
    And, it has everything needed to run the pipeline in one place, as instance parameters and methods.

    Args:
    - year (int): The year for which data is compared.
    - period (int): The month (1-12) or week (1-52) for which data is compared.
    - period_type (str): The type of period ('month' or 'week').
    - report_type (str): The type of report ('impact' or 'kpi').

    Returns:
    - None
    """

    # static variables
    # Define report mapping for each report type : period_type : source
    # precalc source to be added
    report_mapping = {
        'impact': {
            'month': {'api': get_api_impact, 'edw': get_edw_impact,  'table_name': 'impact_qa_monthly_v2'},
            'week': {'api': get_api_impact, 'edw': get_edw_impact, 'table_name': 'impact_qa_weekly'}
                },
        'kpi': {
            'month': {'api': get_api_kpis, 'edw': get_edw_kpis, 'table_name': 'kpi_qa_monthly'},
            'week': {'api': get_api_kpis, 'edw': get_edw_kpis, 'table_name': 'kpi_qa_weekly'}
                }
            }
    
    #add id to postgres 
    #makw new fucntion that read lastest rows (these be two rows now with each id/store)
    #old state vs new state

    def __init__(self, year, period, period_type, report_type, foodbank_name):
        """
        Initialize an instance of the QA Pipeline class.
        """

        logger.info(f"[{foodbank_name}] Initializing Qa_Pipeline...")

        # Set report variables
        self.year = year
        self.period = period
        self.period_type = period_type
        self.report_type = report_type
        self.foodbank_name = foodbank_name

        # Generate run ID and timestamp
        self.run_id = str(uuid.uuid4())
        self.run_timestamp = datetime.utcnow()

        logger.info(f"[{foodbank_name}] Connecting to EDW with retry logic...")

        try:
            self.ldw_engine = connect_with_retry(LDW_CONN_STRING)
            logger.info(f"[{foodbank_name}] Engine created. Testing connection...")

            # Test connection
            with self.ldw_engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            logger.info(f"[{self.foodbank_name}] Connected to EDW successfully.")
        except Exception as e:
            logger.error(f"[{self.foodbank_name}] Failed to connect to EDW after retries: {e}")
            raise



    def calculate_date_range(self):
        '''
        returns start and end date of given week or month
            Args:
        - year (int): Year for which the date range is calculated.
        - period (int): Month (1-12) or Week (1-52) for which the date range is calculated.
        - period_type (str): Type of period ('month' or 'week').

        Returns:
        - tuple: A tuple containing the start and end dates.
        '''
        if self.period_type == 'month':
            if self.period not in range(1, 13):
                raise ValueError("Month must be in 1-12")
            start_date = datetime(self.year, self.period, 1)
            end_date = start_date + relativedelta(months=1) - relativedelta(days=1)
        elif self.period_type == 'week':
            start_date = datetime.strptime(f'{self.year}-W{self.period}-1', "%Y-W%W-%w")
            end_date = start_date + relativedelta(days=6)
        else:
            raise ValueError("Invalid period type. Must be 'month' or 'week'.")

        self.start_date= start_date
        self.end_date= end_date
        logger.info(f"Pipeline Start date: {start_date}, End date: {end_date}")

        print(f"Pipeline Start date: {start_date}, End date: {end_date}")

        return start_date, end_date

    def fetch_data(self):
        '''
        Fetch data from the Foodiverse API and Data Warehouse.
        '''
        logger.info(f"[{self.foodbank_name}] Fetching data...")
        print(f"[{self.foodbank_name}] Fetching data..."); sys.stdout.flush()

        # Fetch data from Foodiverse API
        func = Qa_Pipeline.report_mapping[self.report_type][self.period_type]['api']
        self.api_data = func(self.start_date, self.end_date, self.foodbank_name)

        # Fetch data from Data Warehouse
        with self.ldw_engine.connect() as conn:
            func = Qa_Pipeline.report_mapping[self.report_type][self.period_type]['edw']
            self.edw_data = func(conn, self.start_date, self.end_date, self.foodbank_name)

        print(f"[{self.foodbank_name}] API data: {self.api_data.shape}, EDW data: {self.edw_data.shape}")
        logger.info(f"[{self.foodbank_name}] API data: {self.api_data.shape}, EDW data: {self.edw_data.shape}")
        return self.api_data, self.edw_data



    def preprocess_reports(self):
        '''
        Any preprocessing steps go here
        Args
        - self (object): An instance of the QA Pipeline class.

        Returns:
        - None
        '''

        pass


    def merge_data(self):
        '''
        Merge the data fetched from the all sources.
        Add reference columns

        Args:
        - self (object): An instance of the QA Pipeline class.

        Returns:
        - None
        '''
        merged_df = pd.merge(self.api_data, self.edw_data, 
                             on='official_id', how='outer', suffixes=('_api', '_edw'), 
                             indicator=True, validate='1:1')
        
        # set start and end date, period
        merged_df['start_date'] = self.start_date
        merged_df['end_date'] = self.end_date
        merged_df['period'] = self.period
        merged_df['year'] = self.year

        # Added: run ID and timestamp
        merged_df['run_id'] = self.run_id
        merged_df['run_timestamp'] = self.run_timestamp

        logger.info("Merge Complete")
        print(merged_df['_merge'].value_counts())

        self.merged_df = merged_df

    def merge_data_kpi(self):
        '''
        Merges KPI data from API and EDW sources.
        Ensures one row per official_id (required for 1:1 merge).
        Aggregates and cleans where needed.
        '''

        logger = logging.getLogger(__name__)
        logger.info("Starting KPI merge step...")

        #Standardize column names from API to match EDW
        rename_mapping = {
            "total_transfers": "total_transferred",
            "total_food_offered": "total_food_offered",
            "total_accepted": "total_accepted",
        }
        self.api_data.rename(columns=rename_mapping, inplace=True)

        #Drop exact duplicate rows in API
        before_dedup = self.api_data.shape[0]
        self.api_data.drop_duplicates(inplace=True)
        logger.info(f"Dropped {before_dedup - self.api_data.shape[0]} duplicate rows from API data.")

        #Normalize official_id format
        self.api_data['official_id'] = self.api_data['official_id'].astype(str).str.strip()
        self.edw_data['official_id'] = self.edw_data['official_id'].astype(str).str.strip()

        #Aggregate API data if there are duplicate official_ids
        if self.api_data['official_id'].duplicated().any():
            logger.warning("API has duplicate official_ids aggregating")
            numeric_cols = self.api_data.select_dtypes(include='number').columns.tolist()
            string_cols = ['official_id', 'organisation_name', 'name']
            api_agg = self.api_data.groupby('official_id', as_index=False)[numeric_cols].sum()
            api_meta = self.api_data[string_cols].drop_duplicates(subset='official_id')
            self.api_data = pd.merge(api_agg, api_meta, on='official_id', how='left')

        #Aggregate EDW data if needed
        if self.edw_data['official_id'].duplicated().any():
            logger.warning("EDW has duplicate official_ids aggregating")
            numeric_cols = self.edw_data.select_dtypes(include='number').columns.tolist()
            string_cols = ['official_id', 'branch_name', 'name']
            edw_agg = self.edw_data.groupby('official_id', as_index=False)[numeric_cols].sum()
            edw_meta = self.edw_data[string_cols].drop_duplicates(subset='official_id')
            self.edw_data = pd.merge(edw_agg, edw_meta, on='official_id', how='left')

        #Final validation before merge
        logger.info(f"API data ready for merge: {self.api_data.shape[0]} unique official_ids")
        logger.info(f"EDW data ready for merge: {self.edw_data.shape[0]} unique official_ids")

        # Merge API and EDW
        merged_df = pd.merge(
            self.api_data,
            self.edw_data,
            on='official_id',
            how='outer',
            suffixes=('_api', '_edw'),
            indicator=True,
            validate='1:1'
        )

        # Add metadata
        merged_df['start_date'] = self.start_date
        merged_df['end_date'] = self.end_date
        merged_df['period'] = self.period
        merged_df['year'] = self.year
        merged_df['run_id'] = self.run_id
        merged_df['run_timestamp'] = self.run_timestamp

        logger.info("Merge complete. Merge status counts:")
        logger.info(merged_df['_merge'].value_counts().to_string())

        # Set outputs
        self.merged_df = merged_df
        self.kpi_merged_df = merged_df.copy()




    
    def run_qa_checks(self):
        '''
        Run QA checks on the merged data.

        Args:
        - self (object): An instance of the QA Pipeline class.

        Returns:
        - None
        '''

        # Check if the data is missing in EDW
        self.merged_df['data_missing_edw'] = self.merged_df['_merge'] == 'left_only'

        # Check if the data is missing in FV API data
        self.merged_df['store_missing_fv'] = self.merged_df['_merge'] == 'right_only'

        # calculate qa checks
        self.merged_df['actuals_difference'] = self.merged_df['food_redistributed_actual_api'] - self.merged_df['food_redistributed_actual_edw']
        self.merged_df['effective_difference'] = self.merged_df['food_redistributed_effective_api'] - self.merged_df['food_redistributed_effective_edw']
        self.merged_df['estimated_difference'] = self.merged_df['food_redistributed_estimated_api'] - self.merged_df['food_redistributed_estimated_edw']
        # self.merged_df['abs_difference'] = self.merged_df['difference'].abs()

        # Identify misaligned records
        self.merged_df['actuals_misaligned'] = self.merged_df['actuals_difference'] != 0
        self.merged_df['estimates_misaligned'] = self.merged_df['estimated_difference'] != 0
        self.merged_df['effective_misaligned'] = self.merged_df['effective_difference'] != 0

        # Add a new column to label whether EDW or API has a lesser value
        def determine_difference_label(diff):
            if diff < 0:
                return 'EDW Less'
            elif diff > 0:
                return 'API Less'
            else:
                return 'Equal'

        self.merged_df['actuals_diff_label'] = self.merged_df['actuals_difference'].apply(determine_difference_label)
        self.merged_df['effective_diff_label'] = self.merged_df['effective_difference'].apply(determine_difference_label)
        self.merged_df['estimated_diff_label'] = self.merged_df['estimated_difference'].apply(determine_difference_label)
        
        # Convert differences to absolute values (make all differences positive)
        self.merged_df['actuals_difference'] = self.merged_df['actuals_difference'].abs()
        self.merged_df['effective_difference'] = self.merged_df['effective_difference'].abs()
        self.merged_df['estimated_difference'] = self.merged_df['estimated_difference'].abs()


        logger.info("QA checks complete")

        return None

    
    from sqlalchemy.engine import URL

    def write_to_edw(self):
        '''
        Write the merged data to the database.

        Args:
        - self (object): An instance of the QA Pipeline class.

        Returns:
        - None
        '''
        try:
            ldw_engine = create_engine(
                f"{POSTGRES_ENGINE}/postgres",
                echo=False,
                connect_args={},
                isolation_level="AUTOCOMMIT"  # Enable autocommit mode
            )
            logger.info("connected")
        except Exception as e:
            logger.info("connected failed")
            print('connection failed')
            print(e)
            exit()

        with ldw_engine.connect() as conn:
            self.merged_df.to_sql(
                Qa_Pipeline.report_mapping[self.report_type][self.period_type]['table_name'],
                conn,
                if_exists='append',
                index=False
            )

        logger.info("Data written to EDW")

        print('Data written to EDW')

        return None

    def write_kpi_to_edw(self):
        '''
        Write KPI merged data to the database.
        '''
        try:
            ldw_engine = create_engine(
                f"{POSTGRES_ENGINE}/postgres",
                echo=False,
                connect_args={},
                isolation_level="AUTOCOMMIT"  
            )
            print(' Connected to EDW for KPI data')
        except Exception as e:
            print(' Connection failed:', e)
            return

        #  **Check if Data Exists Before Writing**
        if not hasattr(self, "kpi_merged_df") or self.kpi_merged_df is None:
            print(" No KPI data to write! Ensure KPI merging step ran successfully.")
            return

        if self.kpi_merged_df.empty:
            print(" DataFrame is empty! Nothing to write.")
            return

        print(f" Data Ready for Writing: {self.kpi_merged_df.shape}")

        # **Drop extra columns that are not in PostgreSQL**
        valid_columns = [
            "official_id", "branch_name", "organisation_name",
            "total_food_offered_api", "total_transferred_api", "total_accepted_api",
            "total_food_offered_edw", "total_transferred_edw", "total_accepted_edw",
            "total_food_offered_difference", "total_transferred_difference", "total_accepted_difference",
            "start_date", "end_date", "period", "year",
            "offered_misaligned", "transferred_misaligned", "accepted_misaligned",
            "offered_diff_label", "transferred_diff_label", "accepted_diff_label",
            "run_id", "run_timestamp"  
        ]


        # Keep only columns that exist in the table
        self.kpi_merged_df = self.kpi_merged_df[valid_columns]

        print(f" Columns in DataFrame after cleanup:\n{self.kpi_merged_df.columns.tolist()}")

        # **Write KPI Data to EDW**
        try:
            with ldw_engine.begin() as conn:  #  Ensure transaction commits
                self.kpi_merged_df.to_sql(
                    "kpi_qa_monthly",
                    conn,
                    if_exists='append',
                    index=False
                )
            print(f' KPI Data successfully written to EDW')
        except Exception as e:
            print(f" KPI Data Write Failed: {e}")




    def run_kpi_qa_checks(self):
        '''
        Run QA checks specifically for KPI data.
        '''

        #  Make sure merged columns exist
        expected_columns = ["total_food_offered_edw", "total_transferred_edw", "total_accepted_edw"]
        for col in expected_columns:
            if col not in self.kpi_merged_df.columns:
                raise KeyError(f" Missing column after merge: {col}")

        #  Set metadata columns
        self.kpi_merged_df['start_date'] = self.start_date
        self.kpi_merged_df['end_date'] = self.end_date
        self.kpi_merged_df['period'] = self.period
        self.kpi_merged_df['year'] = self.year

        #  Check if KPI data is missing in EDW
        self.kpi_merged_df['data_missing_edw'] = self.kpi_merged_df['_merge'] == 'left_only'

        #  Check if KPI data is missing in API
        self.kpi_merged_df['store_missing_fv'] = self.kpi_merged_df['_merge'] == 'right_only'

        # Calculate KPI Differences
        self.kpi_merged_df['total_food_offered_difference'] = self.kpi_merged_df['total_food_offered_api'] - self.kpi_merged_df['total_food_offered_edw']
        self.kpi_merged_df['total_transferred_difference'] = self.kpi_merged_df['total_transferred_api'] - self.kpi_merged_df['total_transferred_edw']
        self.kpi_merged_df['total_accepted_difference'] = self.kpi_merged_df['total_accepted_api'] - self.kpi_merged_df['total_accepted_edw']

        #  Identify misaligned KPI records
        self.kpi_merged_df['offered_misaligned'] = self.kpi_merged_df['total_food_offered_difference'] != 0
        self.kpi_merged_df['transferred_misaligned'] = self.kpi_merged_df['total_transferred_difference'] != 0
        self.kpi_merged_df['accepted_misaligned'] = self.kpi_merged_df['total_accepted_difference'] != 0

        #  Add difference labels
        def determine_difference_label(diff):
            if diff < 0:
                return 'EDW Less'
            elif diff > 0:             
                return 'API Less'
            else:
                return 'Equal'

        self.kpi_merged_df['offered_diff_label'] = self.kpi_merged_df['total_food_offered_difference'].apply(determine_difference_label)
        self.kpi_merged_df['transferred_diff_label'] = self.kpi_merged_df['total_transferred_difference'].apply(determine_difference_label)
        self.kpi_merged_df['accepted_diff_label'] = self.kpi_merged_df['total_accepted_difference'].apply(determine_difference_label)

        # Convert differences to absolute values
        self.kpi_merged_df['total_food_offered_difference'] = self.kpi_merged_df['total_food_offered_difference'].abs()
        self.kpi_merged_df['total_transferred_difference'] = self.kpi_merged_df['total_transferred_difference'].abs()
        self.kpi_merged_df['total_accepted_difference'] = self.kpi_merged_df['total_accepted_difference'].abs()

        print(" KPI QA checks complete")
      # print(self.kpi_merged_df.head())  # Show first few rows

        return None



    def run_kpi_pipeline(self):
        '''
        Run the full KPI QA pipeline.
        '''
        self.calculate_date_range()
        self.fetch_data()
        self.preprocess_reports()
        self.merge_data_kpi() 
        self.run_kpi_qa_checks()
        self.write_kpi_to_edw() 
        return None

    def run_qa_pipeline(self):
        print(f"[{self.foodbank_name}] Calculating date range..."); sys.stdout.flush()
        self.calculate_date_range()

        print(f"[{self.foodbank_name}] Fetching data..."); sys.stdout.flush()
        self.fetch_data()

        print(f"[{self.foodbank_name}] Preprocessing..."); sys.stdout.flush()
        self.preprocess_reports()

        print(f"[{self.foodbank_name}] Merging data..."); sys.stdout.flush()
        self.merge_data()

        print(f"[{self.foodbank_name}] Running QA checks..."); sys.stdout.flush()
        self.run_qa_checks()

        print(f"[{self.foodbank_name}] Writing to EDW..."); sys.stdout.flush()
        self.write_to_edw()

        print(f"[{self.foodbank_name}] QA pipeline complete."); sys.stdout.flush()

    
