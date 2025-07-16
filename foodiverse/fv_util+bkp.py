# -*- coding: utf-8 -*-


import requests

import json
import os
import datetime ## for request limiting - get_fn_members
import time 

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

# EMAIL='admin.foodbank@fareshare.org.uk'
EMAIL = 'orgex@foodcloud.ie'
# PW='F00D1ver$e'
PW='F00D1ver$e'

## returns str auth toke, str user uuid, dict complete branches, list branch uuids
## input str email/ppw
def authenticate_fv(EMAIL, PW):

    auth_body={"email":EMAIL,
               "password":PW}
    
    r=requests.post(api_url+"/api/user/login/", params=auth_body)

    if not(str(r.status_code))=='200':
        print('Authentication request failed with status: ', r.status_code, '\n')
        print(r.content)
        return None
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

    return token
    # return token, user, branches, branch_ids

def logout(token):
    logout_url=api_url+'/api/user/logout'
    header={"Authorization":"Bearer "+token}

    r=requests.request('GET',logout_url, headers=header)

    if str(r.status_code)=='200':
        print('Loggout out')
    else:
        print('logout error ', r.status_code)

    return r



## update  windows functions: (get uuid from get_window functions below, and pass to update funcs with vars)
## \\todo day_of_week and times optional orgs, set default to window[day_of_week]?
def update_donation_window(token, nm_uuid, window_uuid, day_of_week, starts_at, ends_at):

    header={"Authorization":"Bearer "+token,
            "Content-Type":"application/json"}

    payload_dict={"day_of_week":day_of_week,
                  "starts_at":starts_at,
                  "ends_at":ends_at}
    payload=json.dumps(payload_dict)
    print(payload)

    if window_uuid=='Not Found':
        create_window_url=api_url+"/api/foodnet_admin/delegated_admin_donor_network_memberships/"+nm_uuid+"/donation_windows/"
        print('posting at ', create_window_url)
        r=requests.request('POST',create_window_url, headers=header, data=payload)

    else:
        update_window_url=api_url+"/api/foodnet_admin/delegated_admin_donor_network_memberships/"+nm_uuid+"/donation_windows/"+window_uuid
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
        create_window_url=api_url+"/api/foodnet_admin/delegated_admin_donor_network_memberships/"+nm_uuid+"/collection_windows/"
        print('posting at ', create_window_url)
        print(payload)
        r=requests.request("POST", create_window_url, headers=header, data=payload)
    else:
        update_window_url=api_url+"/api/foodnet_admin/delegated_admin_donor_network_memberships/"+nm_uuid+"/collection_windows/"+window_uuid
        print('updating at ', update_window_url)
        r=requests.request("PUT",update_window_url, headers=header, data=payload)
        # r='run'

    print(r)

    return r
    ''' delete donation and collection windows '''

def delete_donation_window(token, branch_uuid, window_uuid):

    delete_window_url=api_url+"/api/foodnet_admin/delegated_admin_donor_network_memberships/"+branch_uuid+"/donation_windows/"+window_uuid
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

    delete_window_url=api_url+"/api/foodnet_admin/delegated_admin_donor_network_memberships/"+branch_uuid+"/collection_windows/"+window_uuid
    header={"Authorization":"Bearer "+token,
        "Content-Type":"application/json"}
    print(delete_window_url)
    if window_uuid is not None:
        r=requests.request("DELETE", delete_window_url, headers=header)
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

    branch_data_url=api_url+"/api/foodnet_admin/delegated_admin_donor_network_memberships/"+branch_uuid
    header={"Authorization":"Bearer "+token}
    # print(branch_data_url)
    try:
        r=requests.get(branch_data_url, headers=header)
    except requests.ConnectionError:
        print('Connection Error!!')
        return None

    if str(r.status_code)=="200":
        branch_data=r.json()
        return {'donation_windows':branch_data['data']['donation_windows'], 'collection_windows': branch_data['data']['collection_windows']}
    else:
        print('Get Donation Windows request failed with status: ', r.status_code)
        return {'donation_windows':r, 'collection_windows': r}

def get_donor_donation_windows(token, branch_uuid):

    branch_data_url=api_url+"/api/foodnet_admin/delegated_admin_donor_network_memberships/"+branch_uuid
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

    branch_data_url=api_url+"/api/foodnet_admin/delegated_admin_donor_network_memberships/"+branch_uuid
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
        accept_invitation_url=api_url+'/api/statemachine/collection_window_invitation/'+invitation_uuid+'/transit/accept_invitation'
        r=requests.request('GET', accept_invitation_url, headers=header, data=payload)
        # r='accept'
    return r




'''filter donors and charities from network memberships'''

def get_fn_members(token):

    # request start counter
    i=0
    page_size=50
    ## start time for request limiting
    start_time=datetime.datetime.now()
    fn_chars_url=api_url+"api/foodnet_admin/network_memberships?start="+str(i)+"&length="+str(page_size)
    header={"Authorization":"Bearer "+token}

    payload={}

    r=requests.request("GET", fn_chars_url, headers=header, data = payload)

    members_list=[]

    if str(r.status_code)=='200':
        ## pagination test impl
        fn_members=r.json()

        filtered=fn_members['data']['recordsFiltered']
        members_list.append(fn_members['data']['data'])

        pages, q=divmod(filtered, page_size)

        for page in range(pages):
        # for page in range(1):
            i+=page_size
            fn_chars_url=api_url+"api/foodnet_admin/network_memberships?start="+str(i)+"&length="+str(page_size)
            if i%(page_size*10)==0:
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


def refresh_org_data(token, donors_data_file_location, charities_data_file_localtion):
    members=get_fn_members(token)
    # members_file=os.path.join(cache_path, 'members.json')
    # with open(os.path.dirname(os.path.realpath(__file__))+'\\Cache\\fs\\members.json', 'w', encoding='utf-8') as f:
    # with open(members_file, 'w', encoding='utf-8') as f
    #     json.dump(members, f, ensure_ascii=False, indent=4)

    # fn_charities=[member for member in members if member['type']=='Charity']
    fn_charities=[ { 'official_id':member['branch']['official_id'], 'branch_name':member['branch_name'],  'uuid':member['branch_uuid'] }
                for member in members if member['type']=='Charity']
    print('charity count: ',len(fn_charities) )
    with open(charities_data_file_localtion, 'w', encoding='utf-8') as f:
        json.dump(fn_charities, f, ensure_ascii=False, indent=4)

    # fn_donors=[member for member in members if (member['type']=='Donor' or member['type']=='FoodBank')]
    fn_donors=[ { 'official_id':member['branch']['official_id'], 'branch_name':member['branch_name'],  'uuid':member['uuid'] }  
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

    # windows_request_url=api_url+"/api/user/branches/"+branch['uuid']+"/donation_windows"
    windows_request_url=api_url+"/api/foodnet_admin/delegated_admin_donor_network_memberships/"+branch_uuid
    header={"Authorization":"Bearer "+token}

    windows=requests.get(windows_request_url, headers=header)
    if str(windows.status_code)=='200':
        for window in windows:
            print(window)
    else:
        print('request failed with ', "'",windows.status_code,"'")


def get_webhooks(token):
    webhooks_url=api_url+'/api/organisation/webhooks'
    header={"Authorization":"Bearer "+token}

    webhooks=requests.request('GET', webhooks_url, headers=header)

    if str(webhooks.status_code)=='200':
        print(webhooks)
        print(webhooks.content)
    else:
        print(webhooks.status_code, webhooks.content)

    return webhooks

def set_webhook_rules(token):
    webhooks_url=api_url+'/api/organisation/webhooks'
    header={"Authorization":"Bearer "+token}

    data={'rules':{'events':{
        "donation":[],"offer":[],"membership":[],"collection_window":["Created"],"donation_window":["Created"]
        }}}

    r=requests.request('POST', webhooks_url, headers=header, data=data)

    return r
def get_network_schedules(token, membership_uuid):

    branch_data_url=api_url+"//api/foodnet/network_memberships/"+membership_uuid+"/schedules"
    header={"Authorization":"Bearer "+token}
    print(branch_data_url)
    r=requests.get(branch_data_url, headers=header)

    if str(r.status_code)=="200":
        branch_data=r.json()
        return branch_data['data']
    else:
        print('Get network schedules request failed with status: ', r.status_code)
        return r

def get_branch_tags(token):

    tag_url=api_url+'/api/tags/branch'
    header={"Authorization":"Bearer "+token,
            "Content-Type":"application/json"}

    r=requests.request('GET', tag_url, headers=header)

    # print(r)
    return r





if __name__=="__main__":


    if 'token' in globals():
        print('Already in session')
    else:
        token = authenticate_fv(EMAIL, PW)
    print(token)
    print('Authenticated')
    # token='eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6Ijc3YzIwNGExMzU5MGQyYWI1ZjRiNTMxM2M4N2Q2NjE5NGEwMDY1MDBjNDlmZjYwNGZmYTVkMWUzZWYyNjBjYTUxZTdmYTkyYzJmNmMxNzQ3In0.eyJhdWQiOiIyIiwianRpIjoiNzdjMjA0YTEzNTkwZDJhYjVmNGI1MzEzYzg3ZDY2MTk0YTAwNjUwMGM0OWZmNjA0ZmZhNWQxZTNlZjI2MGNhNTFlN2ZhOTJjMmY2YzE3NDciLCJpYXQiOjE2MzA0MDgzMjksIm5iZiI6MTYzMDQwODMyOSwiZXhwIjoxNjYxOTQ0MzI5LCJzdWIiOiIyMTE0MSIsInNjb3BlcyI6WyIqIl19.Q3hy8EW0oqf2R0CyAK5PHmX0IR5v9Z6GsgkTX9Rb6zKhV9gkSC_Sz-3W5fIQU4cZT7V0kNJF0EUO0DlI0AZT5H-9gGrpUiLBorQ_e4f6A8-_hq3m6Of0mK9C_iT9KbUJXbszvf0fbOCsh9bqHSW3yAizxg7YPlXouTrKQGcRoQitZYMD86AR5C9yt3NpvBwmspW1MSpo26Onny18FQIsfFyMIMHLukkucndSr4j_Eqc2Dpwqg2bwMuMBgdIHA28hI6Ho3FiXwb-8Uxv8Ym8dbaKNGxwSvxz2snbLNbv_tqtSch8N8ZFj9yAFrUxtEGGvo0UGIDnHSQPxwQorlOJTmHwKpxwA44aS_wGyqbP5rl1ilLpG99Ngefk8zo0eY9-egknEXaTAMwftspdPWM7-ysEv3U75sWFA9yDr33Kyj5AyLN9YrwWWCRFSa96_khADf_as84HkmB0nFHVsIovImTld8KgshiMaddpRf0U3PlGRahUlNwyFJaEf50AsJ2Y0mOQ6M4Fmre55kYlFLAh8tM0LZCtOWw9TMq3QxYplk9OCrwkhcWdZNdMflfQNxugrlgXPElFynkx0s_P4djPW0jpYCXYl0GZveH6gijIbF4q_FH9lTVr9adtAEud-joHYl6V31i8zUUWL0skHuvE69m1HYeaCz4nOPkbfcGkNA_0'

    # fn_members_url=api_url+'/api/foodnet/network_memberships?where[0][column]=updated_at&where[0][value]=2019-05-05 00:00:00&where[0][condition]=>'

    # header={"Authorization":"Bearer "+token}

    # payload={}
