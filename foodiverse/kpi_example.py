import logging
import json
import time

import datetime
import requests
import pandas as pd


# api_url='https://api-uat.foodiverse.net/' ## uat env
api_url='https://api-eu.foodiverse.net/'


def authenticate_fv(EMAIL, PW):

    auth_body={"email":EMAIL,
               "password":PW}
    
    r=requests.post(api_url+"/api/user/login/", params=auth_body)

    if not(str(r.status_code))=='200':
        print('Authentication request failed with status: ', r.status_code, '\n')
        print(r.content)
        return r
    print('Authenticated')
    r_json=r.json()
    # print(r_json)

    token=r_json['data']['token']['access_token']
    user=r_json['data']['user']['uuid']
    branches=r_json['data']['user']['branches']
    branch_ids=[]
    for branch in branches:
        branch_ids.append(branch['uuid'])

    # print("token: ", token, "\nuser: ", user, "\nbranches: ", branches, "\nbranch_ids: ", branch_ids)
    # return token, user, branches, branch_ids
    return token


def get_kpi_report(token, start_date, end_date, page_size=100, mode='organisation'):
    '''
    Args:
        token (str): bearer token from fv_authenticate (base_api/login/)
        start_date (str): start date value in yyyy-mm-dd
        end_date (str): end date value in yyyy-mm-dd
        page_size (int): Branches returned in single page. Tech recommends max value of 100
        mode (str): The viewer mode can be branch admin / org admin / foodnet admin / member manager. See FV Reports page for more
    
    return 
        kpi_list (list): list of dicts where each element contains KPI info for a branch

    Desc:
    Call KPI endpoint url
    Get total records from ['recordsFiltered']
    Calculate num pages using records filtered and page size
    Adjust start=<> in API url and loop
    '''

    '''
    Does not implement retry for pages: good idea if fetching many pages'''

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
    kpi_response = requests.request('GET', kpi_url, headers=headers)

    if kpi_response.status_code!=200:
        # return error
        kpi_response

    kpi_json = kpi_response.json()
    # total records
    filtered=kpi_json['data']['recordsFiltered']
    logging.info(f'filtered {filtered} records')
    kpi_list.append(kpi_json['data']['data'])

    pages, q=divmod(filtered, page_size)
    print(f'num pages: {pages}')
    
    # loop through pages 1 to pages
    for page in range(pages):
        i+=page_size
        kpi_url = f'https://api-eu.foodiverse.net/api/reports/branch_compliance_donor/?draw=2&columns%5B0%5D%5Bdata%5D=organisation_name&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=false&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=name&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=false&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=official_id&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=false&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B%5D=&columns%5B46%5D%5Bdata%5D=minDate&columns%5B46%5D%5Bname%5D=&columns%5B46%5D%5Bsearchable%5D=true&columns%5B46%5D%5Borderable%5D=false&columns%5B46%5D%5Bsearch%5D%5Bvalue%5D={start_day_str}T00%3A00%3A00Z&columns%5B46%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B47%5D%5Bdata%5D=maxDate&columns%5B47%5D%5Bname%5D=&columns%5B47%5D%5Bsearchable%5D=true&columns%5B47%5D%5Borderable%5D=false&columns%5B47%5D%5Bsearch%5D%5Bvalue%5D={end_day_str}T23%3A59%3A59Z&columns%5B47%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B48%5D%5Bdata%5D=tag&columns%5B48%5D%5Bname%5D=&columns%5B48%5D%5Bsearchable%5D=true&columns%5B48%5D%5Borderable%5D=false&columns%5B48%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B48%5D%5Bsearch%5D%5Bregex%5D=false&start={i}&length={page_size}&search%5Bvalue%5D=&search%5Bregex%5D=false&parser=datatables&where%5Bmode%5D={mode}&_=1646009329621'
        if i%(page_size*2)==0:
            print('page ', page, '/',pages)

        kpi_response = requests.request('GET', kpi_url, headers=headers)
        kpi_json = kpi_response.json()
        print(f"Appending {len(kpi_json['data']['data'])} records")
        kpi_list.append(kpi_json['data']['data'])

        time.sleep(2.5)

    kpi_list = [e for sublist in kpi_list for e in sublist]

    # filter out not needed keys:
    # impact_list = [{k:v for k,v in e.items() if k not in []} for e in impact_list]

    return kpi_list


if __name__ == '__main__':

    # get auth token
    token = authenticate_fv('email', 'pw')


    # get start / end dates for previous week
    d = datetime.datetime.now()
    iso_cal = d.isocalendar()
    prev_week_start = datetime.datetime.strptime(f"{iso_cal[0]}-W{iso_cal[1]-1}-1", "%Y-W%W-%w")
    prev_week_end = datetime.datetime.strptime(f"{iso_cal[0]}-W{iso_cal[1]-1}-7", "%Y-W%W-%w")


    kpi_list = get_kpi_report(token, prev_week_start, prev_week_end, page_size=100)
    kpi_df = pd.DataFrame(kpi_list)