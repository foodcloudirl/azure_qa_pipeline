'''
Module containing functions to get QA data from the Foodiverse API.
'''

import datetime
from dateutil import relativedelta
from dateutil.relativedelta import relativedelta
import logging
import time
import json
# from azure import functions as func

import pandas as pd
import requests
from sqlalchemy import create_engine, MetaData, Table
import sqlalchemy as sql

from auto_impact_qa.foodiverse import fv_util as fv, fv_db as fvdb

with open('connection_strings.json') as f:
    creds = json.load(f)['foodiverse_users']
    



def get_kpi_report(token, start_date, end_date, page_size=100, mode='members'):
    """
    Retrieves the KPI report data from the API based on the specified parameters.

    Args:
        token (str): The authentication token for accessing the API.
        start_date (datetime.datetime): The start date of the report.
        end_date (datetime.datetime): The end date of the report.
        page_size (int, optional): The number of records to retrieve per page. Defaults to 100.
        mode (str, optional): The mode of the report. Defaults to "members".

    Returns:
        list: A list of impact report data.

    Raises:
        Exception: If there is an error parsing the date variables.
        requests.exceptions.RequestException: If there is an error making the API request.
    """
    

    i=0
    kpi_list=[]

    try:
        start_day_str = datetime.datetime.strftime(start_date, "%Y-%m-%d")
        end_day_str = datetime.datetime.strftime(end_date, "%Y-%m-%d")
    except Exception as e:
        print(e)
        print('couldnt parse date vars')

    # impact_url = "https://api-eu.foodiverse.net/api/reports/offers_impact/?draw=1&columns%5B0%5D%5Bdata%5D=organisation_name&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=false&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=branch_name&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=false&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=official_id&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=false&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=total&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=false&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=total_weight&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=false&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=&columns%5B5%5D%5Bname%5D=&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=false&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=total_co2e&columns%5B6%5D%5Bname%5D=&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=false&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=total_meals&columns%5B7%5D%5Bname%5D=&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=false&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B8%5D%5Bdata%5D=tag&columns%5B8%5D%5Bname%5D=&columns%5B8%5D%5Bsearchable%5D=true&columns%5B8%5D%5Borderable%5D=false&columns%5B8%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B8%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B9%5D%5Bdata%5D=minDate&columns%5B9%5D%5Bname%5D=&columns%5B9%5D%5Bsearchable%5D=true&columns%5B9%5D%5Borderable%5D=false&columns%5B9%5D%5Bsearch%5D%5Bvalue%5D=2022-01-16T00%3A00%3A00Z&columns%5B9%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B10%5D%5Bdata%5D=maxDate&columns%5B10%5D%5Bname%5D=&columns%5B10%5D%5Bsearchable%5D=true&columns%5B10%5D%5Borderable%5D=false&columns%5B10%5D%5Bsearch%5D%5Bvalue%5D=2022-02-15T23%3A59%3A59Z&columns%5B10%5D%5Bsearch%5D%5Bregex%5D=false&start=0&length=10&search%5Bvalue%5D=&search%5Bregex%5D=false&parser=datatable&where%5Bmode%5D=members&_=1644886812819"
    kpi_url = f'https://api-eu.foodiverse.net/api/reports/branch_compliance_donor/?draw=2&columns%5B0%5D%5Bdata%5D=organisation_name&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=false&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=name&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=false&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=official_id&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=false&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B46%5D%5Bdata%5D=minDate&columns%5B46%5D%5Bname%5D=&columns%5B46%5D%5Bsearchable%5D=true&columns%5B46%5D%5Borderable%5D=false&columns%5B46%5D%5Bsearch%5D%5Bvalue%5D={start_day_str}T00%3A00%3A00Z&columns%5B46%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B47%5D%5Bdata%5D=maxDate&columns%5B47%5D%5Bname%5D=&columns%5B47%5D%5Bsearchable%5D=true&columns%5B47%5D%5Borderable%5D=false&columns%5B47%5D%5Bsearch%5D%5Bvalue%5D={end_day_str}T23%3A59%3A59Z&columns%5B47%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B48%5D%5Bdata%5D=tag&columns%5B48%5D%5Bname%5D=&columns%5B48%5D%5Bsearchable%5D=true&columns%5B48%5D%5Borderable%5D=false&columns%5B48%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B48%5D%5Bsearch%5D%5Bregex%5D=false&start={i}&length={page_size}&search%5Bvalue%5D=&search%5Bregex%5D=false&parser=datatables&where%5Bmode%5D={mode}&_=1646009329621'
    headers={"Authorization":"Bearer "+token}
    impact_response = requests.request('GET', kpi_url, headers=headers)

    if impact_response.status_code!=200:
        # return error
        pass

    impact_json = impact_response.json()
    filtered=impact_json['data']['recordsTotal']
    print(f'filtered {filtered}')
    kpi_list.append(impact_json['data']['data'])

    pages, q=divmod(filtered, page_size)
    print(f'num pages: {pages}')
    
    # loop through pages 1 to pages
    for page in range(pages):
        i+=page_size
        kpi_url = f'https://api-eu.foodiverse.net/api/reports/branch_compliance_donor/?draw=2&columns%5B0%5D%5Bdata%5D=organisation_name&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=false&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=name&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=false&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=official_id&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=false&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B46%5D%5Bdata%5D=minDate&columns%5B46%5D%5Bname%5D=&columns%5B46%5D%5Bsearchable%5D=true&columns%5B46%5D%5Borderable%5D=false&columns%5B46%5D%5Bsearch%5D%5Bvalue%5D={start_day_str}T00%3A00%3A00Z&columns%5B46%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B47%5D%5Bdata%5D=maxDate&columns%5B47%5D%5Bname%5D=&columns%5B47%5D%5Bsearchable%5D=true&columns%5B47%5D%5Borderable%5D=false&columns%5B47%5D%5Bsearch%5D%5Bvalue%5D={end_day_str}T23%3A59%3A59Z&columns%5B47%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B48%5D%5Bdata%5D=tag&columns%5B48%5D%5Bname%5D=&columns%5B48%5D%5Bsearchable%5D=true&columns%5B48%5D%5Borderable%5D=false&columns%5B48%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B48%5D%5Bsearch%5D%5Bregex%5D=false&start={i}&length={page_size}&search%5Bvalue%5D=&search%5Bregex%5D=false&parser=datatables&where%5Bmode%5D={mode}&_=1646009329621'
        if i%(page_size*2)==0:
            print('page ', page, '/',pages)

        try:
            impact_response = requests.request('GET', kpi_url, headers=headers)
        except sql.exc.SQLAlchemyError as e:
            print(e)
        except requests.exceptions as e:
            print(e)
        impact_json = impact_response.json()
        print(f"Appending {len(impact_json['data']['data'])} records")
        kpi_list.append(impact_json['data']['data'])

        time.sleep(7.5)

    impact_list = [e for sublist in kpi_list for e in sublist]

    # filter out not needed keys:
    # impact_list = [{k:v for k,v in e.items() if k not in []} for e in impact_list]

    return impact_list

def get_impact_report(token, start_date, end_date, page_size=100, mode="members"):

    """
    Retrieves the impact report data from the API based on the specified parameters.

    Args:
        token (str): The authentication token for accessing the API.
        start_date (datetime.datetime): The start date of the report.
        end_date (datetime.datetime): The end date of the report.
        page_size (int, optional): The number of records to retrieve per page. Defaults to 100.
        mode (str, optional): The mode of the report. Defaults to "members".

    Returns:
        list: A list of impact report data.

    Raises:
        Exception: If there is an error parsing the date variables.
        requests.exceptions.RequestException: If there is an error making the API request.
    """

    i=0
    impact_list=[]

    try:
        start_day_str = datetime.datetime.strftime(start_date, "%Y-%m-%d")
        end_day_str = datetime.datetime.strftime(end_date, "%Y-%m-%d")
    except Exception as e:
        print(e)
        print('couldnt parse date vars')

    # impact_url = "https://api-eu.foodiverse.net/api/reports/offers_impact/?draw=1&columns%5B0%5D%5Bdata%5D=organisation_name&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=false&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=branch_name&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=false&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=official_id&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=false&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=total&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=false&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=total_weight&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=false&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=&columns%5B5%5D%5Bname%5D=&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=false&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=total_co2e&columns%5B6%5D%5Bname%5D=&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=false&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=total_meals&columns%5B7%5D%5Bname%5D=&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=false&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B8%5D%5Bdata%5D=tag&columns%5B8%5D%5Bname%5D=&columns%5B8%5D%5Bsearchable%5D=true&columns%5B8%5D%5Borderable%5D=false&columns%5B8%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B8%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B9%5D%5Bdata%5D=minDate&columns%5B9%5D%5Bname%5D=&columns%5B9%5D%5Bsearchable%5D=true&columns%5B9%5D%5Borderable%5D=false&columns%5B9%5D%5Bsearch%5D%5Bvalue%5D=2022-01-16T00%3A00%3A00Z&columns%5B9%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B10%5D%5Bdata%5D=maxDate&columns%5B10%5D%5Bname%5D=&columns%5B10%5D%5Bsearchable%5D=true&columns%5B10%5D%5Borderable%5D=false&columns%5B10%5D%5Bsearch%5D%5Bvalue%5D=2022-02-15T23%3A59%3A59Z&columns%5B10%5D%5Bsearch%5D%5Bregex%5D=false&start=0&length=10&search%5Bvalue%5D=&search%5Bregex%5D=false&parser=datatable&where%5Bmode%5D=members&_=1644886812819"
    impact_url = f"https://api-eu.foodiverse.net/api/reports/donations_impact/?draw=2&columns%5B0%5D%5Bdata%5D=organisation_name&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=false&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=branch_name&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=false&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=official_id&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=false&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=total&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=false&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=total_weight&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=false&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=&columns%5B5%5D%5Bname%5D=&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=false&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=total_co2e&columns%5B6%5D%5Bname%5D=&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=false&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=total_meals&columns%5B7%5D%5Bname%5D=&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=false&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B8%5D%5Bdata%5D=tag&columns%5B8%5D%5Bname%5D=&columns%5B8%5D%5Bsearchable%5D=true&columns%5B8%5D%5Borderable%5D=false&columns%5B8%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B8%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B9%5D%5Bdata%5D=minDate&columns%5B9%5D%5Bname%5D=&columns%5B9%5D%5Bsearchable%5D=true&columns%5B9%5D%5Borderable%5D=false&columns%5B9%5D%5Bsearch%5D%5Bvalue%5D={start_day_str}T00%3A00%3A00Z&columns%5B9%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B10%5D%5Bdata%5D=maxDate&columns%5B10%5D%5Bname%5D=&columns%5B10%5D%5Bsearchable%5D=true&columns%5B10%5D%5Borderable%5D=false&columns%5B10%5D%5Bsearch%5D%5Bvalue%5D={end_day_str}T23%3A59%3A59Z&columns%5B10%5D%5Bsearch%5D%5Bregex%5D=false&start={i}&length={page_size}&search%5Bvalue%5D=&search%5Bregex%5D=false&parser=datatable&where%5Bmode%5D={mode}&_=1644886812821"
    headers={"Authorization":"Bearer "+token}
    try:
        impact_response = requests.request('GET', impact_url, headers=headers)
    except sql.exc.SQLAlchemyError as e:
        print(e)
        retry = 0
        while retry < 3:
            try:
                impact_response = requests.request('GET', impact_url, headers=headers)
                break 
            except:
                print(f'failed attempt {retry}')
                retry = retry + 1
                time.sleep((retry*5) + 2)

    if impact_response.status_code!=200:
        # return error
        pass

    impact_json = impact_response.json()
    filtered=impact_json['data']['recordsFiltered']
    print(f'filtered {filtered}')
    impact_list.append(impact_json['data']['data'])

    pages, q=divmod(filtered, page_size)
    print(f'num pages: {pages}')
    
    # loop through pages 1 to pages
    for page in range(pages):
        i+=page_size
        impact_url = f"https://api-eu.foodiverse.net/api/reports/donations_impact/?draw=2&columns%5B0%5D%5Bdata%5D=organisation_name&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=false&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=branch_name&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=false&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=official_id&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=false&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=total&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=false&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=total_weight&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=false&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=&columns%5B5%5D%5Bname%5D=&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=false&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=total_co2e&columns%5B6%5D%5Bname%5D=&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=false&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=total_meals&columns%5B7%5D%5Bname%5D=&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=false&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B8%5D%5Bdata%5D=tag&columns%5B8%5D%5Bname%5D=&columns%5B8%5D%5Bsearchable%5D=true&columns%5B8%5D%5Borderable%5D=false&columns%5B8%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B8%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B9%5D%5Bdata%5D=minDate&columns%5B9%5D%5Bname%5D=&columns%5B9%5D%5Bsearchable%5D=true&columns%5B9%5D%5Borderable%5D=false&columns%5B9%5D%5Bsearch%5D%5Bvalue%5D={start_day_str}T00%3A00%3A00Z&columns%5B9%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B10%5D%5Bdata%5D=maxDate&columns%5B10%5D%5Bname%5D=&columns%5B10%5D%5Bsearchable%5D=true&columns%5B10%5D%5Borderable%5D=false&columns%5B10%5D%5Bsearch%5D%5Bvalue%5D={end_day_str}T23%3A59%3A59Z&columns%5B10%5D%5Bsearch%5D%5Bregex%5D=false&start={i}&length={page_size}&search%5Bvalue%5D=&search%5Bregex%5D=false&parser=datatable&where%5Bmode%5D={mode}&_=1644886812821"
        if i%(page_size*2)==0:
            print('page ', page, '/',pages)

        impact_response = requests.request('GET', impact_url, headers=headers)
        impact_json = impact_response.json()
        print(f"Appending {len(impact_json['data']['data'])} records")
        impact_list.append(impact_json['data']['data'])

        time.sleep(7.5)

    impact_list = [e for sublist in impact_list for e in sublist]

    # filter out not needed keys:
    # impact_list = [{k:v for k,v in e.items() if k not in []} for e in impact_list]

    return impact_list

def preprocess(df):
    """
    Preprocesses cleanup JSON to strings before inserting into the database.

    Args:
        df (pandas.DataFrame): The DataFrame to be preprocessed.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame.
    """
    extra_cols= [ 'in_line_address','tags','country','province', 'county', 'countries_id','provinces_id', 'counties_id', 
                 'planned_donation_scheduled', 'donor_branch_compliance' ]
    for col in extra_cols:
        if col in df.columns:
            df.drop(col, axis=1, inplace=True)

    # rename impact columns
    try:
        df.rename(columns={'actual_total_weight':'food_redistributed_actual', 'total_weight':'food_redistributed_effective', 
                       'estimated_total_weight':'food_redistributed_estimated'}, inplace=True)
    except Exception as e:
        print(e)
        print('error in renaming Impact columns, skpped')

    return df

def get_api_impact(start_date, end_date, foodbank_name):
    '''
    calls get_imact_report for given foodbank_name and start / end date
    '''

    foodiverse_user=creds[foodbank_name]
    fb_token = fv.authenticate_fv(foodiverse_user['username'], foodiverse_user['password'])

    impact_list = get_impact_report(fb_token, start_date, end_date)
    impact_df = pd.DataFrame(impact_list)

    impact_df = preprocess(impact_df)

    return impact_df

def get_api_kpis(start_date, end_date, foodbank_name):
    '''
    calls get_kpi_report for given foodbank_name and start / end date
    '''
    
    foodiverse_user=creds[foodbank_name]
    fb_token = fv.authenticate_fv(foodiverse_user['username'], foodiverse_user['password'])

    kpi_list = get_kpi_report(fb_token, start_date, end_date)
    kpi_df = pd.DataFrame(kpi_list)

    kpi_df = preprocess(kpi_df)

    return kpi_df


if __name__ == '__main__':
    fb_token = fv.authenticate_fv('orgex@foodcloud.ie', 'F00D1ver$e')

    try:
        engine = create_engine('postgresql+psycopg2://dbadmin:{}@fv-rep-v1.postgres.database.azure.com:5432/postgres?sslmode=require', echo=False)
        meta = MetaData()
        conn = engine.connect()
        logging.info('Connected to destination db')
    except:
        logging.error('Connection to destination db failed')
        meta = None
        engine = None

    months = [1, 2]
    year = 2023
    
    ## stuff

    print('insert weekly records complete')