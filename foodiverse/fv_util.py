# -*- coding: utf-8 -*-


import requests
# from sqlalchemy import desc

import json
import os
import datetime ## for request limiting - get_fn_members
import time
import logging

## patch requests.send to set default timeout
DEFAULT_TIMEOUT = 180

old_send = requests.Session.send

def new_send(*args, **kwargs):
     if kwargs.get("timeout", None) is None:
         kwargs["timeout"] = DEFAULT_TIMEOUT
     return old_send(*args, **kwargs)

requests.Session.send = new_send


# api_url='https://api-uat.foodiverse.net/'
api_url='https://api-eu.foodiverse.net/'
# api_url='https://api-qa.foodiverse.net/'

EMAIL='admin.foodbank@fareshare.org.uk'
# EMAIL = 'orgex@foodcloud.ie'
# PW='F00D1ver$e'
PW='F00D1ver$e'

## returns str auth toke, str user uuid, dict complete branches, list branch uuids
## input str email/ppw
def authenticate_fv(EMAIL, PW):

    auth_body={"email":EMAIL,
               "password":PW}
    
    r=requests.post(api_url+"api/user/login/", params=auth_body)

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

def logout(token):
    logout_url=api_url+'api/user/logout'
    header={"Authorization":"Bearer "+token}

    r=requests.request('GET',logout_url, headers=header)

    if str(r.status_code)=='200':
        print('Loggout out')
    else:
        print('logout error ', r.status_code)

    return r



## update  windows functions: (get uuid from get_window functions below, and pass to update funcs with vars)
## \\todo day_of_week and times optional orgs, set default to window[day_of_week]?
def update_donation_window(token, nm_uuid, window_uuid, day_of_week, starts_at, ends_at, is_active=1):

    header={"Authorization":"Bearer "+token,
            "Content-Type":"application/json"}

    payload_dict={"day_of_week":day_of_week,
                  "starts_at":starts_at,
                  "ends_at":ends_at,
                  "is_active":is_active}
    payload=json.dumps(payload_dict)
    print(payload)

    if window_uuid=='Not Found':
        create_window_url=api_url+"api/foodnet_admin/delegated_admin_donor_network_memberships/"+nm_uuid+"/donation_windows/"
        print('posting at ', create_window_url)
        r=requests.request('POST',create_window_url, headers=header, data=payload)

    else:
        update_window_url=api_url+"api/foodnet_admin/delegated_admin_donor_network_memberships/"+nm_uuid+"/donation_windows/"+window_uuid
        print('updating at ', update_window_url)
        r=requests.request('PUT', update_window_url, headers=header, data=payload)

    print(r)

    return r


    '''
    creating payload to update collection window requires input from get_collection_window. 
    For ex, to remove a charity from waitlist, the remove_waitlist arg requires the relevant json from the waitlist returned in collection_windows
    (Although - collection_windows retuens list of windows for each day. below function takes one element of this list as collection_window arg)


    Args:
        collection_window: element of collection_windows list returned in get_donor_collection_windows
        remove_primary: boolean, remove primary if exists
        all others: uuid of charity \\todo this should take list of uuids to invite/remove multiple. Specially important as we need one row / donor / day for the rest of funcs
    '''
def collection_window_payload_builder(collection_window, remove_primary, remove_waitlist, uninvite, add_waitlist, invite_to, invitation_starts_at):

    if collection_window is None:
        primary_payload=None
        remove_waitlist_payload=[]
        uninvite_payload=None
        primary_info=None
        remove_waitlist_info=None
        uninvite_info=None

        if add_waitlist is None or str(add_waitlist)=='nan':
            #replace with pd.isna
            add_waitlist_payload=[]
        else:
            add_waitlist_payload=[{"charity_branch":{"uuid":add_waitlist}}]
        if invite_to is None or str(invite_to)=='nan':
            invite_to_payload={'charity_branch':{'uuid':None}}
        else:
            if invitation_starts_at is None or str(invitation_starts_at)=='nan':
                invite_to_payload={'charity_branch':{'uuid':invite_to}}
            else:
                invite_to_payload={'starts_at_date':invitation_starts_at, "starts_at_x_days_from_acceptance":None, "expires_at":None, 'charity_branch':{'uuid':invite_to}}
        add_waitlist_info=None
        invite_to_info=None

    else:


         if remove_primary=='remove':
             primary_payload, primary_info=payload_remove_primary(collection_window)
         else :
             primary_payload = None
             primary_info=None
    
    
         if remove_waitlist==None or str(remove_waitlist)=='nan':
             remove_waitlist_payload=[]
             remove_waitlist_info=None
         else: remove_waitlist_payload, remove_waitlist_info=payload_remove_waitlist(collection_window['collection_window_waitlist'], remove_waitlist)
    
         if add_waitlist==None or str(add_waitlist)=='nan':
             add_waitlist_payload=[]
             add_waitlist_info=None
         else: add_waitlist_payload, add_waitlist_info=payload_add_waitlist(collection_window['collection_window_waitlist'], add_waitlist)
    
         if uninvite=='uninvite':
             uninvite_payload, uninvite_info=payload_uninvite(collection_window['invited_branch'])
         else:
             uninvite_payload=None
             uninvite_info=None
    
         if invite_to==None or str(invite_to)=='nan':
             invite_to_payload=None
             invite_to_info=None
         else: invite_to_payload, invite_to_info=payload_invite_to(collection_window['invited_branch'], invite_to, invitation_starts_at)

    payload={'removed_primary':primary_payload,
              'uninvited':uninvite_payload,
              'to_invite':invite_to_payload,
              'added_waitlist':add_waitlist_payload,
              'remove_waitlist':remove_waitlist_payload}

    info_list=[primary_info, uninvite_info, invite_to_info, add_waitlist_info, remove_waitlist_info]
    ''' flatten list of lists '''
    info_list=[info for info in info_list if info]
    info_str='\n'.join(info_list)
    print(payload)
    return payload, info_str




def payload_remove_primary(collection_window):

    primary=collection_window['primary_charity_branch']

    ##primary list null?
    if primary is None:
        info='No Primary Charity'
        primary_payload=None
    else:
        info='None'
        primary_payload=primary

    return primary_payload, info

## get_collection_windows()[i]['waitlist'] list of dicts
def payload_add_waitlist(collection_window_waitlist, add_charity_uuid):
    # waitlist=collection_window['charity_window_waitlist']

    if len(collection_window_waitlist)==0:
        add_waitlist_payload=[{"charity_branch":{"uuid":add_charity_uuid}}]
        info=None
        return add_waitlist_payload, info

    else:
        ##check if in waitlist
        wl_ids=[]
        for wl in collection_window_waitlist:
            wl_ids.append(wl['charity_branch']['uuid'])

        if add_charity_uuid in wl_ids:
            add_waitlist_payload=[]
            info=add_charity_uuid + 'already in waitlist'
            return add_waitlist_payload, info
        else:
            add_waitlist_payload=[{"charity_branch":{"uuid":add_charity_uuid}}]
            info=None
            return add_waitlist_payload, info

def payload_remove_waitlist(collection_window_waitlist, remove_charity_uuid):

    if len(collection_window_waitlist)==0:
        remove_waitlist_payload=[]
        info='No charities in waitlist'
        return remove_waitlist_payload, info

    else:
        ## set to not found; loop through windows; if found update value and break
        remove_waitlist_payload=[]
        info=remove_charity_uuid + 'not found in waitlist'

        for wl in collection_window_waitlist:
            if wl['charity_branch']['uuid']==remove_charity_uuid:
                remove_waitlist_payload=[wl]
                info=None
                break

    return remove_waitlist_payload, info


def payload_uninvite(collection_window_invite):

    if collection_window_invite is None:
        uninvite_payload=None
        info='No charity invitation'
    else:
        uninvite_payload=collection_window_invite
        info=None

    return uninvite_payload, info

def payload_invite_to(collection_window_invite, invite_charity_uuid, invitation_starts_at=None):

    if collection_window_invite is None:
        invite_to_payload={'starts_at_date':invitation_starts_at, "starts_at_x_days_from_acceptance":None, "expires_at":None, 'charity_branch':{'uuid':invite_charity_uuid}}
        info=None
    else:
        ## if already invited, skip
        invited_charity_uuid=collection_window_invite['charity_branch']['uuid']
        if invited_charity_uuid==invite_charity_uuid:
            invite_to_payload={'charity_branch':None}
            info=invite_charity_uuid + 'already invited'
        else:
            ##another charity is invited
            invite_to_payload={'starts_at_date':invitation_starts_at, "starts_at_x_days_from_acceptance":None, "expires_at":None, 'charity_branch':{'uuid':invite_charity_uuid}}
            info="Another Charity is already invited. Ensure that 'Uninvite' is set to 'Uninvite' to proceed with this update"
    return invite_to_payload, info

'''
required args: token,nm_uuid, window_uuid (set to 'Not Found' to Post new window), start/end dow, (starts_at, ends_at %H:%M:%S format strings)
optional rargs: transfer_types_id (default=1: Collection)
max_collectors default=1
removed_primary: find out request format (T/F?)
removed_waitlist

'''
def update_collection_window_timings(token, nm_uuid, window_uuid, day_of_week_start, day_of_week_end, starts_at, ends_at,
                                     transfer_types_id=1, max_collectors=1,
                                     removed_primary=None, removed_waitlist=[], uninvited=None,
                                     added_waitlist=[], to_invite={"charity_branch":None}):

    header={"Authorization":"Bearer "+token,
            "Content-Type":"application/json"}

    payload_dict={"transfer_types_id":transfer_types_id, ## for now. optional arg to fetch and replace if null
                  "day_of_week_end":day_of_week_end,
                  "day_of_week_start":day_of_week_start,
                  "difference_in_days":day_of_week_end-day_of_week_start, ##for now, \\todo replace with mod subtraction
                  "starts_at":starts_at,
                  "ends_at":ends_at,
                  "max_collectors":max_collectors, ## for now. optional arg to fetch and replace if null
                  "removed_primary":removed_primary,
                  "removed_waitlist":removed_waitlist,
                  "uninvited":uninvited,
                  "added_waitlist":added_waitlist,
                  "to_invite":to_invite
                  }

    payload=json.dumps(payload_dict)
    print(payload)

    if window_uuid=='Not Found':
        create_window_url=api_url+"api/foodnet_admin/delegated_admin_donor_network_memberships/"+nm_uuid+"/collection_windows/"
        print('posting at ', create_window_url)
        print(payload)
        r=requests.request("POST", create_window_url, headers=header, data=payload)
    else:
        update_window_url=api_url+"api/foodnet_admin/delegated_admin_donor_network_memberships/"+nm_uuid+"/collection_windows/"+window_uuid
        print('updating at ', update_window_url)
        r=requests.request("PUT",update_window_url, headers=header, data=payload)
        # r='run'

    print(r)

    return r
    ''' delete donation and collection windows '''

def delete_donation_window(token, branch_uuid, window_uuid):

    delete_window_url=api_url+"api/foodnet_admin/delegated_admin_donor_network_memberships/"+branch_uuid+"/donation_windows/"+window_uuid
    header={"Authorization":"Bearer "+token,
        "Content-Type":"application/json"}
    print(delete_window_url)
    if window_uuid is not None:
        r=requests.request("DELETE", delete_window_url, headers=header)
        print(r)
    else:
        r=None

    return r

def delete_collection_window(token, branch_uuid, window_uuid):

    delete_window_url=api_url+"api/foodnet_admin/delegated_admin_donor_network_memberships/"+branch_uuid+"/collection_windows/"+window_uuid
    header={"Authorization":"Bearer "+token,
        "Content-Type":"application/json"}
    print(delete_window_url)
    if window_uuid is not None:
        r=requests.request("DELETE", delete_window_url, headers=header)
        time.sleep(1)
        print(r)
    else:
        r='No Window'

    return r

## return dict containing foodnet donor members data
def get_fn_donor_members(token):

    # temp="https://api-uat.food.cloud/api/foodnet_admin/delegated_admin_donor_network_memberships/b1267003-74c5-466a-993e-5b4f65130c37/donation_windows/658b97f4-0860-47e0-afd9-4b4bfb6c9954"
    ## test pagination
    i=0
    page_size=10

    donor_members_url=api_url+"api/foodnet_admin/delegated_admin_donor_network_memberships/?start="+str(i)+"&length="+str(page_size)
    header={"Authorization":"Bearer "+token}

    r=requests.get(donor_members_url, headers=header)
    donors_list=[]
    if str(r.status_code)=="200":
        donors=r.json()
        donors_list.append(donors['data']['data'])
        filtered=donors['data']['recordsFiltered']
        pages, q=divmod(filtered, page_size)

        for i in range(pages):
            i+=page_size
            donor_members_url=api_url+"api/foodnet_admin/delegated_admin_donor_network_memberships/?start="+str(i)+"&length="+str(page_size)
            r=requests.get(donor_members_url, headers=header)
            donors=r.json()
            donors_list.append(donors['data']['data'])
        return [donor for sublist in donors_list for donor in sublist ]
    else:
        print('Donor members request failed with status: ', r.status_code)
        return r

## windows for a single branch , with branch_uuid input. Returns dict with list for each day's window
## each day-list with a dict for each window.
## for updates using this, we are using just the first window, but reading all to handle later

def get_donor_windows(token, branch_uuid):

    branch_data_url=api_url+"api/foodnet_admin/delegated_admin_donor_network_memberships/"+branch_uuid
    header={"Authorization":"Bearer "+token}
    # print(branch_data_url)
    try:
        r=requests.get(branch_data_url, headers=header)
    except:
        pass
    
    try:
        if str(r.status_code)=="200":
            branch_data=r.json()
            return {'donation_windows':branch_data['data']['donation_windows'], 'collection_windows': branch_data['data']['collection_windows']}
        else:
            print('Get Donation Windows request failed with status: ', r.status_code)
            return {'donation_windows':r, 'collection_windows': r}
    except:
        return None

def get_donor_donation_windows(token, branch_uuid):

    branch_data_url=api_url+"api/foodnet_admin/delegated_admin_donor_network_memberships/"+branch_uuid
    header={"Authorization":"Bearer "+token}
    # print(branch_data_url)
    r=requests.get(branch_data_url, headers=header)

    if str(r.status_code)=="200":
        branch_data=r.json()
        return branch_data['data']['donation_windows']
    else:
        print('Get Donation Windows request failed with status: ', r.status_code)
        return r

def get_donor_collection_windows(token, branch_uuid):

    branch_data_url=api_url+"api/foodnet_admin/delegated_admin_donor_network_memberships/"+branch_uuid
    header={"Authorization":"Bearer "+token}

    r=requests.get(branch_data_url, headers=header)

    if str(r.status_code)=='200':
        branch_data=r.json()
        return branch_data['data']['collection_windows']

    else:
        print('Get Collection Windows request failed with status: ', r.status_code)
        print(r.content)
        print(branch_data_url)
        return r



'''  function to accept charity invitations.
    Required args: charity uuid and dow
    Optional args: 1. collection window uuid: if given, validate and fire accept api
                   2. donor uuid: if collection window uuid not given, get charity schedules for given charity. If invitation available at given dow, accept. What if more than one?
    window and uuid not procided: get schedules. If one and only one invitation available, accept. if not, what? raise and leace it there for now?
'''

def accept_invitation(charity_token, invitation_uuid):

    header={"Authorization":"Bearer "+charity_token,
            'Referrer-Policy':'strict-origin-when-cross-orogin'}
    payload={}

    if invitation_uuid==None:
        r='error'


    else:
        accept_invitation_url=api_url+'api/statemachine/collection_window_invitation/'+invitation_uuid+'/transit/accept_invitation'
        r=requests.request('GET', accept_invitation_url, headers=header, data=payload)
        # r='accept'
    return r




'''filter donors and charities from network memberships'''

def get_fn_members(token, start_date=None):

    # request start counter
    i=0
    page_size=50
    ## start time for request limiting
    start_time=datetime.datetime.now()
    if start_date is None:
        fn_chars_url=api_url+f"api/foodnet_admin/network_memberships?start={i}&length={page_size}"
    else:
        fn_chars_url=api_url+f"api/foodnet_admin/network_memberships?where[0][column]=updated_at&where[0][value]={start_date}&where[0][condition]=>&start={i}&length={page_size}"
        # fn_chars_url=api_url+f"api/foodnet_admin/network_memberships?where[0][column]=updated_at&where[0][value]={start_date}"

    header={"Authorization":"Bearer "+token}

    payload={}

    r=requests.request("GET", fn_chars_url, headers=header, data = payload)

    members_list=[]

    if str(r.status_code)=='200':
        ## pagination test impl
        fn_members=r.json()

        filtered=fn_members['data']['recordsFiltered']
        print(f'filtered {filtered}')
        members_list.append(fn_members['data']['data'])

        pages, q=divmod(filtered, page_size)
        print(pages)
        for page in range(pages):
        # for page in range(20,22,1):
            i+=page_size
            if start_date is None:
                fn_chars_url=api_url+f"api/foodnet_admin/network_memberships?start={i}&length={page_size}"
            else:
                fn_chars_url=api_url+f"api/foodnet_admin/network_memberships?where[0][column]=updated_at&where[0][value]={start_date}&where[0][condition]=>&start={i}&length={page_size}"
            print(fn_chars_url)
            if i%(page_size*2)==0:
                # print('page ', i/10, '/',pages)
                print('page ', page, '/',pages)
            r=requests.request("GET", fn_chars_url, headers=header, data = payload)
            fn_members=r.json()
            members_list.append(fn_members['data']['data'])

            ## rate limit 20 pages/min
            if page%20==0:
                now_time=datetime.datetime.now()
                time_diff=(start_time-now_time).seconds
                # if complete in less than 60 secinds, wait till end of 60s
                if time_diff < 60:
                    time.sleep(60-time_diff)    

        # print(fn_members['data'])
        # return fn_members['data']['data']
        return [member for sublist in members_list for member in sublist]

    else:
        print('Get Foodnet Members in get_fn_charities() request failed with status: ', r.status_code)
        return r

def get_fn_charities(token):

    members=get_fn_members(token)

    # print('name', 'branch mame', 'type', sep='...')
    fn_charities=[]
    for member in members:
        # print(member['name'],member['branch_name'], member['branch_uuid'], member['type'], sep=' ... ')
        print(member['type'])
        if member['type']=='Charity':
            fn_charities.append(member)

    # print(fn_charities)
    return fn_charities

def get_fn_donors(token):

    members=get_fn_members(token)

    # print('name', 'branch mame', 'type', sep='...')
    fn_charities=[]
    for member in members:
        # print(member['name'],member['branch_name'], member['branch_uuid'], member['type'], sep=' ... ')
        print(member['type'])
        if member['type']=='Donor' or member['type']=='FoodBank':
            fn_charities.append(member)

    # print(fn_charities)
    return fn_charities

def refresh_charity_data(token, charity_data_file_location):
    if os.path.isfile(charity_data_file_location): ## todo also check age here
        print('found charity data file \n\n')
        with open(charity_data_file_location, encoding='utf-8') as file:
            charities_data=json.load(file)
    else:
        resp_data=get_fn_charities(token)

        charities_data={}
        for charity in resp_data:
            # print(donor, '\n\n')
            data={'name':charity['name'],
                  'branch_name':charity['branch_name'],
                  'branch_uuid':charity['branch_uuid'],
                  'official_id':charity['branch']['official_id'],
                  'uuid':charity['uuid']
                  }
            charities_data[charity['branch_name']]=data ## \\todo change key to ext id
        with open(charity_data_file_location, 'w', encoding='utf-8') as f:
            json.dump(charities_data, f, ensure_ascii=False, indent=4)
    return charities_data

def refresh_donor_data(token, donors_data_file_location):
    if os.path.isfile(donors_data_file_location): ## todo also check age here
        print('found donor data file \n\n')
        with open(donors_data_file_location) as file:
            donors_data=json.load(file)
    else:
        # resp_data=get_fn_donor_members(token)
        resp_data=get_fn_donors(token)

        donors_data={}
        for donor in resp_data:
            # print(donor, '\n\n')
            data={'org_name':donor['name'],
                  'branch_name':donor['branch_name'],
                  'nm_uuid':donor['branch_uuid'],
                  'official_id':donor['network']['branch']['official_id'],
                  'uuid':donor['uuid']
                  }
            donors_data[donor['branch_name']]=data ## \\todo change key to ext id
        with open(donors_data_file_location, 'w', encoding='utf-8') as f:
            json.dump(donors_data, f, ensure_ascii=False, indent=4)
    return donors_data


def refresh_org_data(token, donors_data_file_location, charities_data_file_localtion, start_date=None):
    members=get_fn_members(token, start_date)
    # members_file=os.path.join(cache_path, 'members.json')
    # with open(os.path.dirname(os.path.realpath(__file__))+'\\Cache\\fs\\members.json', 'w', encoding='utf-8') as f:
    # with open(members_file, 'w', encoding='utf-8') as f
    #     json.dump(members, f, ensure_ascii=False, indent=4)

    # fn_charities=[member for member in members if member['type']=='Charity']
    fn_charities=[ { 'official_id':member['branch']['official_id'], 'branch_name':member['branch_name'],  'uuid':member['branch_uuid'], 'nm_uuid':member['uuid'] }
                for member in members if member['type']=='Charity']
    print('charity count: ',len(fn_charities) )
    with open(charities_data_file_localtion, 'w', encoding='utf-8') as f:
        json.dump(fn_charities, f, ensure_ascii=False, indent=4)

    # fn_donors=[member for member in members if (member['type']=='Donor' or member['type']=='FoodBank')]
    fn_donors=[ { 'official_id':member['branch']['official_id'], 'branch_name':member['branch_name'],  'uuid':member['uuid'], 'branch_uuid':member['branch']['uuid'] }  
            for member in members if (member['type']=='Donor' or member['type']=='FoodBank')]
    print('donor count: ',len(fn_donors) )
    with open(donors_data_file_location, 'w', encoding='utf-8') as f:
        json.dump(fn_donors, f, ensure_ascii=False, indent=4)

    # donors_data={}
    # charities_data={}

    # for donor in fn_donors:
    #     # print(donor, '\n\n')
    #     data={'org_name':donor['name'],
    #               'branch_name':donor['branch_name'],
    #               'nm_uuid':donor['branch_uuid'],
    #               # 'official_id':donor['network']['organization']['official_id'],
    #               'official_id':donor['branch']['official_id'],
    #               'uuid':donor['uuid']
    #               }
    #     donors_data[donor['branch_name']]=data ## \\todo change key to ext id
    # with open(donors_data_file_location, 'w', encoding='utf-8') as f:
    #     json.dump(donors_data, f, ensure_ascii=False, indent=4)

    # for charity in fn_charities:
    #     # print(donor, '\n\n')
    #     data={'name':charity['name'],
    #               'branch_name':charity['branch_name'],
    #               'branch_uuid':charity['branch_uuid'],
    #               'official_id':charity['branch']['official_id'],
    #               'uuid':charity['uuid']
    #               }
    #     charities_data[charity['branch_name']]=data ## \\todo change key to ext id
    # with open(charities_data_file_localtion, 'w', encoding='utf-8') as f:
    #     json.dump(charities_data, f, ensure_ascii=False, indent=4)
    # return donors_data, charities_data
    return fn_donors, fn_charities

def get_db_members(token):
    members = get_fn_members(token, start_date=None)

    members_list=[db_members_fields(member) for member in members]

    return members_list

## input str token, dict branch (returned from authenticate)

def printCharities(token, branch):
    print("Charities in Branch ", branch['name'],": ")

    charities_request_url=api_url+"api/user/branches/"+branch['uuid']+"/charities"
    header={"Authorization":"Bearer "+token}

    charities=requests.get(charities_request_url, headers=header)
    if charities.status_code==200:
        charities_json=charities.json()
        for charity in charities_json['data']['data']:
            print(charity['org_name'])
    else:
        print('charities request failed with ', charities.status_code)

    return None

def printBranchWindows(token, branch_uuid):

    # windows_request_url=api_url+"api/user/branches/"+branch['uuid']+"/donation_windows"
    windows_request_url=api_url+"api/foodnet_admin/delegated_admin_donor_network_memberships/"+branch_uuid
    header={"Authorization":"Bearer "+token}

    windows=requests.get(windows_request_url, headers=header)
    if str(windows.status_code)=='200':
        for window in windows:
            print(window)
    else:
        print('request failed with ', "'",windows.status_code,"'")


def get_webhooks(token):
    webhooks_url=api_url+'api/organisation/webhooks'
    header={"Authorization":"Bearer "+token}

    webhooks=requests.request('GET', webhooks_url, headers=header)

    if str(webhooks.status_code)=='200':
        print(webhooks)
        print(webhooks.content)
    else:
        print(webhooks.status_code, webhooks.content)

    return webhooks

def set_webhook_rules(token):
    webhooks_url=api_url+'api/organisation/webhooks'
    header={"Authorization":"Bearer "+token}

    data={'rules':{'events':{
        "donation":[],"offer":[],"membership":[],"collection_window":["Created"],"donation_window":["Created"]
        }}}

    r=requests.request('POST', webhooks_url, headers=header, data=data)

    return r
def get_network_schedules(token, membership_uuid):

    branch_data_url=api_url+"api/foodnet/network_memberships/"+membership_uuid+"/schedules"
    header={"Authorization":"Bearer "+token}
    print(branch_data_url)
    r=requests.get(branch_data_url, headers=header)

    if str(r.status_code)=="200":
        branch_data=r.json()
        # return branch_data['data']
        return branch_data
    else:
        print('Get network schedules request failed with status: ', r.status_code)
        return r

def get_branch_tags(token):

    tag_url=api_url+'api/tags/branch'
    header={"Authorization":"Bearer "+token,
            "Content-Type":"application/json"}

    r=requests.request('GET', tag_url, headers=header)

    # print(r)
    return r

def get_network_member(token, membership_uuid):
    
    url=api_url+f'api/foodnet_admin/network_memberships/{membership_uuid}'
    header={"Authorization":"Bearer "+token,
            "Content-Type":"application/json"}
    r=requests.get(url, headers=header)
    print(r)
    if r.status_code==200:
        nm_data=r.json()['data']
        # print(nm_data)
        member=db_members_fields(nm_data)
        return r, member
    else:
        print(f'Error getting membership: {r.status_code} : {r.content}')
        return None, None

def db_members_fields(nm_data):
    '''get required db fields from memberships api response
        args:
        member dict api response
        return:
        member dict with keys corresponding to db headers'''
    member={}
    member['nm_uuid']=nm_data['uuid']
    try:
        member['type']=nm_data['branch']['organisation']['organisation_type']['name']
    except KeyError:
        member['type']=nm_data['type']
    member['network_uuid']=nm_data['network']['uuid']
    member['network_org_uuid']=nm_data['network']['organisation']['uuid']
    member['network_org_name']=nm_data['network']['organisation']['name']
    member['current_status']=nm_data['tsm_current_state']
    member['created_at']=nm_data['created_at']
    member['updated_at']=nm_data['updated_at']
    member['branch_uuid']=nm_data['branch']['uuid']
    member['branch_name']=nm_data['branch']['name']
    member['storage_types']=nm_data['branch']['storage_types']
    member['branch_isactive']=nm_data['branch']['is_active']
    addr_keys=['countries_id', 'provinces_id', 'counties_id', 'address_1', 'address_2', 'city_town', 'post_code', 'latitude', 'longitude', 'official_id']
    for key in addr_keys:
        member[key]=nm_data['branch'][key]
    return member

# def insert_member(member, conn, table):
#     #set start time to now()
#     member['start_time']=datetime.datetime.now()
#     member['end_time']=None
#     nm_uuid=member['nm_uuid']

#     query= f"select * from members where nm_uuid='{member['nm_uuid']} order by start_time desc limit (1) "
#     query=table.select().where(table.c.nm_uuid==member['nm_uuid']).order_by(desc('start_time')).first()
#     slct=conn.execute(query)
#     if slct.rowcount!=0:
#         for row in slct:
#             id=row['id']
        
        

def get_donations(token,days_previous, start_days=14, page_size=50):

    # temp="https://api-uat.food.cloud/api/foodnet_admin/delegated_admin_donor_network_memberships/b1267003-74c5-466a-993e-5b4f65130c37/donation_windows/658b97f4-0860-47e0-afd9-4b4bfb6c9954"
    ## test pagination
    i=0    
    # start_date=date.today() - timedelta(days=14)
    # end_date=date.today() - timedelta(days=days_previous)
    # start_date=start_date.strftime('%Y/%m/%d')
    # end_date=end_date.strftime('%Y/%m/%d')
    # logging.info("Start_Date:"+start_date)
    # logging.info("End_Date:"+end_date)
    
    start_date=datetime.datetime.today() - datetime.timedelta(days=start_days)
    end_date=datetime.datetime.today() - datetime.timedelta(days=days_previous)
    start_date=start_date.strftime('%Y/%m/%d')
    end_date=end_date.strftime('%Y/%m/%d')
    print("Start_Date:"+start_date)
    print("End_Date:"+end_date)
    donor_members_url="https://api-eu.foodiverse.net/api/donations?draw=12&columns%5B0%5D%5Bdata%5D=&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=false&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=branch_name&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=false&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=rag_status&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=false&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=donation_date&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=false&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=donation_time&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=false&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=ambient_total&columns%5B5%5D%5Bname%5D=&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=false&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=chilled_total&columns%5B6%5D%5Bname%5D=&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=false&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=frozen_total&columns%5B7%5D%5Bname%5D=&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=false&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B8%5D%5Bdata%5D=hot_total&columns%5B8%5D%5Bname%5D=&columns%5B8%5D%5Bsearchable%5D=true&columns%5B8%5D%5Borderable%5D=false&columns%5B8%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B8%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B9%5D%5Bdata%5D=time_left&columns%5B9%5D%5Bname%5D=&columns%5B9%5D%5Bsearchable%5D=true&columns%5B9%5D%5Borderable%5D=false&columns%5B9%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B9%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B10%5D%5Bdata%5D=tsm_current_state&columns%5B10%5D%5Bname%5D=&columns%5B10%5D%5Bsearchable%5D=true&columns%5B10%5D%5Borderable%5D=false&columns%5B10%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B10%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B11%5D%5Bdata%5D=&columns%5B11%5D%5Bname%5D=&columns%5B11%5D%5Bsearchable%5D=true&columns%5B11%5D%5Borderable%5D=false&columns%5B11%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B11%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B12%5D%5Bdata%5D=external_id&columns%5B12%5D%5Bname%5D=&columns%5B12%5D%5Bsearchable%5D=true&columns%5B12%5D%5Borderable%5D=false&columns%5B12%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B12%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B13%5D%5Bdata%5D=official_id&columns%5B13%5D%5Bname%5D=&columns%5B13%5D%5Bsearchable%5D=true&columns%5B13%5D%5Borderable%5D=false&columns%5B13%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B13%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B14%5D%5Bdata%5D=minDate&columns%5B14%5D%5Bname%5D=&columns%5B14%5D%5Bsearchable%5D=true&columns%5B14%5D%5Borderable%5D=false&columns%5B14%5D%5Bsearch%5D%5Bvalue%5D="+start_date+"&columns%5B14%5D%5Bsearch%5D%5Bregex%5D=true&columns%5B15%5D%5Bdata%5D=maxDate&columns%5B15%5D%5Bname%5D=&columns%5B15%5D%5Bsearchable%5D=true&columns%5B15%5D%5Borderable%5D=false&columns%5B15%5D%5Bsearch%5D%5Bvalue%5D="+end_date+"&columns%5B15%5D%5Bsearch%5D%5Bregex%5D=true&columns%5B16%5D%5Bdata%5D=tag&columns%5B16%5D%5Bname%5D=&columns%5B16%5D%5Bsearchable%5D=true&columns%5B16%5D%5Borderable%5D=false&columns%5B16%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B16%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B17%5D%5Bdata%5D=branch_uuid&columns%5B17%5D%5Bname%5D=&columns%5B17%5D%5Bsearchable%5D=true&columns%5B17%5D%5Borderable%5D=false&columns%5B17%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B17%5D%5Bsearch%5D%5Bregex%5D=false&search%5Bvalue%5D=&search%5Bregex%5D=false&parser=datatables&_=1616666006104"+ "&start=" +str(i)+"&length="+str(page_size)
    header={"Authorization":"Bearer "+token}
    print (donor_members_url)
    r=requests.get(donor_members_url, headers=header)
    donors_list=[]
    response_list = []
    if str(r.status_code)=="200":
        donors=r.json()
        donors_list.append(donors['data']['data'])
        filtered=donors['data']['recordsFiltered']
        pages, q=divmod(filtered, page_size)
        print(filtered)
        t=0
        for i in range(pages):
            t = i*page_size
            print(i)
            print(t)
            print(page_size)
            time.sleep(1.5)
            donor_members_url="https://api-eu.foodiverse.net/api/donations?draw=12&columns%5B0%5D%5Bdata%5D=&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=false&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=branch_name&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=false&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=rag_status&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=false&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=donation_date&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=false&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=donation_time&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=false&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=ambient_total&columns%5B5%5D%5Bname%5D=&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=false&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=chilled_total&columns%5B6%5D%5Bname%5D=&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=false&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=frozen_total&columns%5B7%5D%5Bname%5D=&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=false&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B8%5D%5Bdata%5D=hot_total&columns%5B8%5D%5Bname%5D=&columns%5B8%5D%5Bsearchable%5D=true&columns%5B8%5D%5Borderable%5D=false&columns%5B8%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B8%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B9%5D%5Bdata%5D=time_left&columns%5B9%5D%5Bname%5D=&columns%5B9%5D%5Bsearchable%5D=true&columns%5B9%5D%5Borderable%5D=false&columns%5B9%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B9%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B10%5D%5Bdata%5D=tsm_current_state&columns%5B10%5D%5Bname%5D=&columns%5B10%5D%5Bsearchable%5D=true&columns%5B10%5D%5Borderable%5D=false&columns%5B10%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B10%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B11%5D%5Bdata%5D=&columns%5B11%5D%5Bname%5D=&columns%5B11%5D%5Bsearchable%5D=true&columns%5B11%5D%5Borderable%5D=false&columns%5B11%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B11%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B12%5D%5Bdata%5D=external_id&columns%5B12%5D%5Bname%5D=&columns%5B12%5D%5Bsearchable%5D=true&columns%5B12%5D%5Borderable%5D=false&columns%5B12%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B12%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B13%5D%5Bdata%5D=official_id&columns%5B13%5D%5Bname%5D=&columns%5B13%5D%5Bsearchable%5D=true&columns%5B13%5D%5Borderable%5D=false&columns%5B13%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B13%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B14%5D%5Bdata%5D=minDate&columns%5B14%5D%5Bname%5D=&columns%5B14%5D%5Bsearchable%5D=true&columns%5B14%5D%5Borderable%5D=false&columns%5B14%5D%5Bsearch%5D%5Bvalue%5D="+start_date+"&columns%5B14%5D%5Bsearch%5D%5Bregex%5D=true&columns%5B15%5D%5Bdata%5D=maxDate&columns%5B15%5D%5Bname%5D=&columns%5B15%5D%5Bsearchable%5D=true&columns%5B15%5D%5Borderable%5D=false&columns%5B15%5D%5Bsearch%5D%5Bvalue%5D="+end_date+"&columns%5B15%5D%5Bsearch%5D%5Bregex%5D=true&columns%5B16%5D%5Bdata%5D=tag&columns%5B16%5D%5Bname%5D=&columns%5B16%5D%5Bsearchable%5D=true&columns%5B16%5D%5Borderable%5D=false&columns%5B16%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B16%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B17%5D%5Bdata%5D=branch_uuid&columns%5B17%5D%5Bname%5D=&columns%5B17%5D%5Bsearchable%5D=true&columns%5B17%5D%5Borderable%5D=false&columns%5B17%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B17%5D%5Bsearch%5D%5Bregex%5D=false&search%5Bvalue%5D=&search%5Bregex%5D=false&parser=datatables&_=1616666006104"+"&start=" +str(t)+"&length="+str(page_size)
            r=requests.get(donor_members_url, headers=header)
            donors=r.json()
                        
            
            for donation in donors['data']['data']:
                don =0
                if 'W' in donation['responses']:
                    for response in donation['responses']['W']:
                        response_list.append(response)                  
                if 'P' in donation['responses']:
                    for response in donation['responses']['P']:
                        response_list.append(response)
                if 'F' in donation['responses']:
                    for response in donation['responses']['F']:
                        response_list.append(response)
            donors_list.append(donors['data']['data'])
        return [donor for sublist in donors_list for donor in sublist ] , response_list
    else:
        print('Donor members request failed with status: ', r.status_code)
        return r, r.content


def _get_impact_report(token, start_date, end_date, page_size=100, mode="members"):

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
    impact_response = requests.request('GET', impact_url, headers=headers)

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

def _get_kpi_report(token, start_date, end_date, page_size=100, mode='members'):
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

def send_slack(slack_url, message):

    headers = {'Content-type':'application/json'}
    data = f"{{'text':'{message}'}}"

    r = requests.post(slack_url, headers = headers, data = data)

    return r

def get_kpi_report(token, start_date, end_date, page_size=100, mode='members'):
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
        except requests.exceptions as e:
            print(e)
        except requests.exceptions as e:
            print(e)
        try:
            impact_json = impact_response.json()
        except Exception as e:
            print(e)
            print('JSON Decode error; skipping page')
            continue
        print(f"Appending {len(impact_json['data']['data'])} records")
        kpi_list.append(impact_json['data']['data'])

        ## if end, break
        if len(impact_json['data']['data'])==0:
            break

        time.sleep(1.5)

    impact_list = [e for sublist in kpi_list for e in sublist]

    # filter out not needed keys:
    # impact_list = [{k:v for k,v in e.items() if k not in []} for e in impact_list]

    return impact_list

def get_impact_report(token, start_date, end_date, page_size=100, mode="members"):

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
    except requests.exceptions as e:
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

if __name__=="__main__":


    # if 'token' in globals():
    #     print('Already in session')
    # else:
    #     # token = authenticate_fv("rourke+testingfoodbank@foodcloud.ie", "12345678Aa")
    #     token = authenticate_fv('orgex@foodcloud.ie', 'F00D1ver$e')
    #     print('Authenticated')

    # # url=api_url+"/api/donations/ea9a2af4-dcac-485a-b9f6-3ad520097f37/?with=responses.items.donation_item.category,responses.charity_branch.memberships.storage_types,responses.donation_collection_window"
    # # r=requests.get(api_url, headers={"Authorization":"Bearer "+token,
    # #                                 "Content-Type":"application/json"})
    # # print(r.content)

    # # r=update_collection_window_timings(token, nm_uuid="b13fe574-e11c-46f5-b760-cfbc13a1e9d6", window_uuid='Not Found', day_of_week_start=1, day_of_week_end=1, starts_at="10:00:00", ends_at="12:00:00",
    # #                                  transfer_types_id=1, max_collectors=1,
    # #                                  removed_primary=None, removed_waitlist=[], uninvited=None,
    # #                                  added_waitlist=[], to_invite={"charity_branch":"e762e22f-568a-430b-ba24-460b8b964660"})
    # # nm_id='60b8fd98-6331-414a-8e9f-1f0b9d28b3fd'
    # # url=f"https://api-eu.foodiverse.net/api/user/branches/{nm_id}"

    # # r=requests.get(api_url, headers={"Authorization":"Bearer "+token,
    # #                                 "Content-Type":"application/json"})
    # # print(r.content)
    # # token='eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjU0NjM0YmNmNGZmMDczNTdiMTRhNDJhZjRmMTFkNGJhZWJiMGEyNDIzNzhmMTE4NTFhYTAyYWEyMmM1ZWEwN2IwOTU5ZmZhNGI2MDdkNWY3In0.eyJhdWQiOiIyIiwianRpIjoiNTQ2MzRiY2Y0ZmYwNzM1N2IxNGE0MmFmNGYxMWQ0YmFlYmIwYTI0MjM3OGYxMTg1MWFhMDJhYTIyYzVlYTA3YjA5NTlmZmE0YjYwN2Q1ZjciLCJpYXQiOjE2MzI5MzMwNTUsIm5iZiI6MTYzMjkzMzA1NSwiZXhwIjoxNjMzNTM3ODU1LCJzdWIiOiIxMCIsInNjb3BlcyI6WyIqIl19.UWtX7OLuu9VP2GomQBSldaIikYPulQ-KXNZqbP_SHezdMNMUwBQac-Qt99d3JJQUz9yYFLT2RZ80vv_4BTx0kqkVJZLbrZPguG92CMreJRhFqjHuh1hUAEMjlKUgpqnVRqoQ8wVQhfH5TvqgTxhZaKkj6dpNGD2mwT_3rKvTfOJgz2tkfAqrNd7u2QOuaUvHILpNu-pilborxX1vI4MI2cP_X37x6ev6pL-bz-6kj0hXrV4QJiQpJlNLZJ3RsCdrZfG26ucTJko3PI3rvOLUNlSQAUwHNBdaHetze3zTtxU0pvm8dKpPN0XuDQp8UbjoBiwlUXOntIKVJqHzmYukGu8Smba8-h2fs_UADzK1y6_S0YkVYd617DisDr7vXuo-lGq8m7l3nOIQ7MXdQ39GlTNtcd5U2kKT6VvxU82N_xCPx21WqunzfOosAiyxMgp3xzRWbJrux83mrMZGRB-b5Vf7-i9M9k7p063DlqYFhfMeUj3rkM0BslNbOF5cOU3iCm6zjGdCUko_eAD7fWbWobXFbwJnUXkiWD05dqVcoPy_PPlwXCS99rJ4igsM1s37jomVzSC54rRLh_Z7QyuGNm8y4mM6M4JKKSdo9BkwlGceMz2n20_ov9DjFFzYOWBxyVJWoWZ9NoDrU5Y6a0TIBhcCx6MNqIDS653DJf4X3Xo'
    # r = get_donor_windows(token, 'c95cb12d-cf08-41d5-937d-953ee4902688')

    s = send_slack('https://hooks.slack.com/services/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', 'Test')

    