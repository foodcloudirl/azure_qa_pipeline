# -*- coding: utf-8 -*-

try:
    import foodiverse.fv_util as fv
except:
    import fv_util as fv

import foodiverse.fv_db as fvdb

import os
import sys
import json
import time

import pandas as pd
import datetime
import requests
import numpy as np



## convert to dt.dt if valid time string, if not, return error string
def strToTime(x, time_format, time_error_text='Time Parse Error'):
    try:
        return datetime.datetime.strptime(x, time_format).time()
    except ValueError:

        return time_error_text
    except TypeError:
        return None

def validate_donor_exists(input_data, donors_df):
    '''get uuids with join on donor'''
    input_data=input_data.merge(donors_df, how='left', left_on='Key', right_on='official_id')
    input_data['DonorNotFoundFlag']=None
    input_data['DonorNotFoundFlag'].loc[input_data.uuid.isnull()]='Donor ID Not Found'
    return input_data

def validate_charity_exists(input_data, charities_df, on):

    input_data=input_data.merge(charities_df, how='left', left_on=on, right_on='official_id', suffixes=(None, '_'+on))
    col=on+'_CharityNotFound'
    input_data[col]=''

    input_data[col].loc[(input_data['uuid_'+on].isnull()) & (input_data[on]!='')]=on+' Charity ID Not found'


    # for col in input.loc[:,['Donation Starts At', 'Donation Ends At', 'Collection Starts At', 'Collection Ends At']]:
    #     print(col.dtype)
    return input_data


def validateTimeFormat(input_data, time_format):

    timecols_list=['Donation Starts At', 'Donation Ends At', 'Collection Starts At', 'Collection Ends At']
    time_error_text='Time Format Error' ## error to print in value, and later search for when filtering error logs
    input_data['InvalidTimeFormatFlag']=None

    timecols=input_data.loc[:,timecols_list]
    for colnum, col in enumerate(timecols.columns):
        ##convert to dt if valid time string, if not, return error string
        parsed_col=col+'_Parsed'
        input_data[parsed_col]=timecols[col].apply(lambda x: strToTime(x, time_format, time_error_text))

    ## From parsed cols, update 'Time Format Error' Flag Column if error found.
    ## \\todo This  is double loop with above; consider changing
    parsecols_list=[col+'_Parsed' for col in timecols_list]
    parsecols=input_data.loc[:, parsecols_list]
    input_data['Time Format Error']=parsecols.apply(lambda x: 'Invalid Time' if time_error_text in x.values else None, axis=1)
    ##\\todo for rows without error, check if start<end, return t/f?
    # print(input_data.to_string())
    ## check dow in [0:6]
    valid_dows=[n for n in range(7)]
    input_data['ValidDayOfWeek']=input_data.apply(lambda x: None if x.DayOfWeek in valid_dows else 'Day Of Week should be between 0 (Monday) and 6 (Sunday)', axis=1)

    return input_data


# def get_fn_schedules(fvuser, donor_suffix=None, donors_list=None):
#     ''' function to get donor schedules available to current user.
#         trasform output in format usable for InputSchedule
#     '''
#     donors_df=fvuser.donors_df
#     print('total len', len(donors_df))
#     if donor_suffix is not None:
#         donors_df=donors_df.loc[fvuser.donors_df.name.str.startswith(donor_suffix),:]
#         # donors_df=donors_df.loc[fvuser.donors_df.name.str.startswith(('Tesco', 'Aldi', 'Dunnes', 'Lidl', 'Musgrave'))]
#     print('after suffix: ', len(donors_df))
#     if donors_list is not None:
#         if not(isinstance(donors_list, list)):
#             print('donor_list argument should be type list containing donors ids')
#         else:
#             donors_df=donors_df.loc[donors_df.official_id.isin(donors_list), :]
#     print('Filtered donors: ', len(donors_df))
#     ## groupby donor and get_window
#     scheds=donors_df.groupby('official_id').apply(lambda x: get_window_data(fvuser.token, x.uuid.iloc[0]))
#     columns=['uuid', 'DayOfWeek', 'DonationWindowId', 'Donation Starts At', 'Donation Ends At',
#             'CollectionWindowId', 'Collection Starts At', 'Collection Ends At',
#             'primary', 'primary_uuid', 'primary_name', 'storage', 'primary_start_date',
#             'invite_to', 'invite_to_uuid', 'invite_to_name', 'invite_start_date', 'waitlist']
#     # scheds_df=pd.DataFrame(columns=columns)
#     # for sched in scheds:
#     #     sched_df=pd.DataFrame(sched, columns=columns)
#     #     scheds_df=pd.concat([scheds_df, sched_df])

#     ## short cut for to_list -> pd.df() -> melt i.e. each row containing list of lists converted to series while preserving index
#     # print(scheds)
#     pandasmagic=scheds.explode()
#     schedules=pd.DataFrame(pandasmagic.to_list(), index=pandasmagic.index, columns=columns)

def get_fn_schedules(fvuser, donor_suffix=None, donors_list=None, windows=None):
    ''' function to get donor schedules available to current user.
        trasform output in format usable for InputSchedule
    '''
    donors_df=fvuser.donors_df
    print('total len', len(donors_df))
    if donor_suffix is not None:
        donors_df=donors_df.loc[fvuser.donors_df.official_id.str.startswith(donor_suffix, na=False),:]
        # donors_df=donors_df.loc[fvuser.donors_df.name.str.startswith(('Tesco', 'Aldi', 'Dunnes', 'Lidl', 'Musgrave'))]
    print('after suffix: ', len(donors_df))
    if donors_list is not None:
        if not(isinstance(donors_list, list)):
            print('donor_list argument should be type list containing donors ids')
        else:
            donors_df=donors_df.loc[donors_df.official_id.isin(donors_list), :]
            print(f'len after filt : {len(donors_df)}')
    print('Filtered donors: ', len(donors_df))
    ## groupby donor and get_window
    scheds=donors_df.groupby('official_id').apply(lambda x: get_window_data(fvuser.token, x.uuid.iloc[0], windows))
    columns=['uuid', 'DayOfWeek', 'DonationWindowId', 'Donation Starts At', 'Donation Ends At',
            'CollectionWindowId', 'Collection Starts At', 'Collection Ends At',
            'primary', 'primary_uuid', 'primary_name', 'storage', 'primary_start_date',
            'invite_to', 'invite_to_uuid', 'invite_to_name', 'invite_start_date', 'waitlist']
    # scheds_df=pd.DataFrame(columns=columns)
    # for sched in scheds:
    #     sched_df=pd.DataFrame(sched, columns=columns)
    #     scheds_df=pd.concat([scheds_df, sched_df])

    ## short cut for to_list -> pd.df() -> melt i.e. each row containing list of lists converted to series while preserving index
    # print(scheds)
    pandasmagic=scheds.explode()
    schedules=pd.DataFrame(pandasmagic.to_list(), index=pandasmagic.index, columns=columns)

    def get_posting_day(x):
        try:
            is_morning_collection=x['Collection Starts At']<x['Donation Starts At']
            print(is_morning_collection, ' ismorning collection')
            if is_morning_collection:
                posting_day=(x.DayOfWeek-1)%7
            else:
                posting_day=x.DayOfWeek
        except:
            posting_day=x.DayOfWeek

        return posting_day

    
    # schedules['Posting Day'] = schedules.apply(lambda x: (x.DayOfWeek-1)%6 if x['Collection Starts At']<x['Donation Starts At'] else x.DayOfWeek, axis=1 ).astype(str)
    schedules['Posting Day'] = schedules.apply(get_posting_day, axis=1).astype(str)
    schedules['DayOfWeek'] = schedules.DayOfWeek.astype(str)

    ## get_windows_data returns list of lists for waitlists. Current Schedules uploader required a single waitlist value per row. So, further explode() waitlists and copy to separate df
    ## preserved identifier columns as index
    wl_cols=['official_id','uuid', 'DayOfWeek', 'Posting Day', 'CollectionWindowId', 'waitlist']
    # wls_df=schedules.reset_index().loc[:, wl_cols].set_index(['official_id','uuid', 'DayOfWeek', 'CollectionWindowId'])
    wls_df=schedules.reset_index().reindex(columns=wl_cols).set_index(['official_id','uuid', 'DayOfWeek', 'Posting Day', 'CollectionWindowId'])
    wls_flat=wls_df.waitlist.explode()
    ## investigate strange explode() behavious when nulls in series: should work as normal acc to docs (https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.explode.html)
    wls_flat=wls_flat.loc[pd.notna(wls_flat),:]
    print(wls_flat)

    if len(wls_flat)>0:
        wls=pd.DataFrame(wls_flat.to_list(), index=wls_flat.index, columns=['add_waitlist', 'uuid_add_waitlist'])
    else: 
        wls=pd.Series(wls_flat, name='add_waitlist')

    return schedules, wls

# def get_window_data(token, donor_uuid):
#     ## to call for each donor
#     print(f'Fetching schedules for {donor_uuid}')
#     windows=fv.get_donor_windows(token, donor_uuid)
#     print(windows)
#     donation_windows=windows['donation_windows']
#     collection_windows=windows['collection_windows']
#     time.sleep(1)

#     donor_schedules=[]

#     for dow in range(7):
#         try:
#             donation_window=donation_windows[dow]
#                     ## if donation window exists
#             if len(donation_windows[dow])>0:
#                 ## pick first if multiple windpws
#                 donation_window=donation_windows[dow][0]
#                 donation_window_uuid=donation_window['uuid']
#                 donation_starts_at=donation_window['starts_at']
#                 donation_ends_at=donation_window['ends_at']
#             else:
#                 donation_window_uuid = donation_starts_at = donation_ends_at =None

#         except TypeError:
#             [donation_window, donation_window_uuid, donation_starts_at, donation_ends_at]=['Error']*4


#         try:
#             collection_window=collection_windows[dow]
#             if len(collection_windows[dow])>0:
#             ##init empty waitlist list
#                 waitlist_list=[]
#                 collection_window=collection_windows[dow][0]
#                 collection_window_uuid=collection_window['uuid']
#                 collection_starts_at=collection_window['starts_at']
#                 collection_ends_at=collection_window['ends_at']
#                 if collection_window['primary_charity_branch'] is None:
#                     primary=None
#                     primary_uuid=None
#                     primary_name=None
#                     primary_storage=None
#                     primary_start_date=None
#                 else:
#                     primary=collection_window['primary_charity_branch']['charity_branch']['official_id']
#                     primary_uuid=collection_window['primary_charity_branch']['charity_branch']['uuid']
#                     primary_name=collection_window['primary_charity_branch']['charity_branch']['name']
#                     primary_storage=collection_window['primary_charity_branch']['charity_branch']['storage_types']
#                     primary_start_date=collection_window['primary_charity_branch']['starts_at_date']
    
#                 if collection_window['invited_branch'] is None:
#                     invite_to=None
#                     invite_to_uuid=None
#                     invite_to_name=None
#                     invite_start_date=None
#                 else:
#                     invite_to=collection_window['invited_branch']['charity_branch']['official_id']
#                     invite_to_uuid=collection_window['invited_branch']['charity_branch']['uuid']
#                     invite_to_name=collection_window['invited_branch']['charity_branch']['name']
#                     invite_start_date=collection_window['invited_branch']['starts_at_date']
    
#                 if len(collection_window['collection_window_waitlist'])==0:
#                     pass
#                 else:
#                     waitlist=collection_window['collection_window_waitlist']
#                     for wl in waitlist:
#                         waitlist_list.append([wl['charity_branch']['official_id'], wl['charity_branch']['uuid']])
    
#             else:
#                 collection_window_uuid = collection_starts_at = collection_ends_at = primary = primary_uuid = primary_name = primary_storage = primary_start_date = invite_to = invite_to_uuid = invite_to_name = invite_start_date=None
#                 waitlist_list=[]

#         except TypeError:
#             [collection_window_uuid , collection_starts_at , collection_ends_at ,
#              primary , primary_uuid , primary_name, primary_storage, primary_start_date,  invite_to , invite_to_uuid, invite_to_name, invite_start_date]=['Error']*12
#             waitlist_list=[]


       
#         donor_schedules.append([donor_uuid, dow, donation_window_uuid, donation_starts_at, donation_ends_at,
#                                 collection_window_uuid, collection_starts_at, collection_ends_at, 
#                                 primary, primary_uuid, primary_name, primary_storage, primary_start_date,
#                                 invite_to, invite_to_uuid, invite_to_name, invite_start_date, waitlist_list])


#     return donor_schedules

def get_window_data(token, donor_uuid, windows_dict=None, write_to_dict=None, name=None):
    ## to call for each donor
    print(f'Fetching schedules for {donor_uuid} : {name}')
    if windows_dict is None:
        windows=fv.get_donor_windows(token, donor_uuid)
        time.sleep(1)
    else:
        try:
            windows=windows_dict[donor_uuid]
        except KeyError:
            windows={}
            windows['donation_windows']=[[],[],[],[],[],[],[]]
            windows['collection_windows']=[[],[],[],[],[],[],[]]
    # print(windows)
    donation_windows=windows['donation_windows']
    collection_windows=windows['collection_windows']
    

    donor_schedules=[]

    for dow in range(7):
        try:
            donation_window=donation_windows[dow]
                    ## if donation window exists
            if len(donation_windows[dow])>0:
                ## pick first if multiple windpws
                donation_window=donation_windows[dow][0]
                donation_window_uuid=donation_window['uuid']
                donation_starts_at=donation_window['starts_at']
                donation_ends_at=donation_window['ends_at']
            else:
                donation_window_uuid = donation_starts_at = donation_ends_at =None

        except TypeError:
            [donation_window, donation_window_uuid, donation_starts_at, donation_ends_at]=['Error']*4


        try:
            collection_window=collection_windows[dow]
            if len(collection_windows[dow])>0:
            ##init empty waitlist list
                waitlist_list=[]
                collection_window=collection_windows[dow][0]
                collection_window_uuid=collection_window['uuid']
                collection_starts_at=collection_window['starts_at']
                collection_ends_at=collection_window['ends_at']
                if collection_window['primary_charity_branch'] is None:
                    primary=None
                    primary_uuid=None
                    primary_name=None
                    primary_storage=None
                    primary_start_date=None
                else:
                    primary=collection_window['primary_charity_branch']['charity_branch']['official_id']
                    primary_uuid=collection_window['primary_charity_branch']['charity_branch']['uuid']
                    primary_name=collection_window['primary_charity_branch']['charity_branch']['name']
                    try:
                        primary_storage=collection_window['primary_charity_branch']['charity_branch']['storage_types']
                    except:
                        primary_storage=None
                    primary_start_date=collection_window['primary_charity_branch']['starts_at_date']
    
                if collection_window['invited_branch'] is None:
                    invite_to=None
                    invite_to_uuid=None
                    invite_to_name=None
                    invite_start_date=None
                else:
                    invite_to=collection_window['invited_branch']['charity_branch']['official_id']
                    invite_to_uuid=collection_window['invited_branch']['charity_branch']['uuid']
                    invite_to_name=collection_window['invited_branch']['charity_branch']['name']
                    invite_start_date=collection_window['invited_branch']['starts_at_date']
    
                if len(collection_window['collection_window_waitlist'])==0:
                    pass
                else:
                    waitlist=collection_window['collection_window_waitlist']
                    for wl in waitlist:
                        try:
                            waitlist_list.append([wl['charity_branch']['official_id'], wl['charity_branch']['uuid']])
                        except KeyError:
                            ##if source db/azure
                            waitlist_list.append([wl['branch_official_id'], wl['branch_uuid']])
    
            else:
                collection_window_uuid = collection_starts_at = collection_ends_at = primary = primary_uuid = primary_name = primary_storage = primary_start_date = invite_to = invite_to_uuid = invite_to_name = invite_start_date=None
                waitlist_list=[]

        except TypeError:
            [collection_window_uuid , collection_starts_at , collection_ends_at ,
             primary , primary_uuid , primary_name, primary_storage, primary_start_date,  invite_to , invite_to_uuid, invite_to_name, invite_start_date]=['Error']*12
            waitlist_list=[]


       
        donor_schedules.append([donor_uuid, dow, donation_window_uuid, donation_starts_at, donation_ends_at,
                                collection_window_uuid, collection_starts_at, collection_ends_at, 
                                primary, primary_uuid, primary_name, primary_storage, primary_start_date,
                                invite_to, invite_to_uuid, invite_to_name, invite_start_date, waitlist_list])

        if write_to_dict is not None:
            try:
                write_to_dict.append(donor_schedules)
            except Exception as e:
                print(f'write to dict fauled: {e}')

        # time.sleep(0.5)


    return donor_schedules

def get_official_id(nm_uuid, token):
    r, member = fvdb.get_network_member(token, nm_uuid)
    time.sleep(0.2)
    if member is None:
        official_id = None
    else:
        official_id = member['official_id']
        # if official_id:
            # r = requests.post(fvdb.update_membership_url)
    print(f'{nm_uuid}: {official_id}')

    return official_id



class FvUser_db :

    def __init__(self, EMAIL, PW, conn, fn_name, cache_path=None, auto=False):
        self.Email = EMAIL
        self.PW = PW

        if 'fareshare' in self.Email.lower():
            self.fn_name='fareshare'
        elif 'foodcloud' in self.Email.lower():
            self.fn_name='foodcloud'
        else: self.fn_name=fn_name

        self.token=fv.authenticate_fv(EMAIL, PW)

        if not(isinstance(self.token, str)) or self.token is None:
            print('exiting..')
            # return None
            sys.exit()


        # donors_q = f"SELECT official_id, branch_name, nm_uuid as uuid, branch_uuid from memberships where end_time is null and network_org_name = '{fn_name}' and type = 'Donor' ;"
        # charities_q = f"SELECT official_id, branch_name, branch_uuid as uuid, nm_uuid from memberships where end_time is null and network_org_name = '{fn_name}' and type = 'Charity' ;"

        # self.donors_df = pd.read_sql_query(donors_q, conn)
        # self.charities_df = pd.read_sql_query(charities_q, conn)
        self.members = self.get_members_azure(fn_name)
        try:
            print(self.members.status_code, self.members.content)
        except:
            pass 
        print(self.members.keys())
        # members = json.loads(members)
        donors = self.members['donors']
        charities = self.members['charities']
        self.donors_df = pd.DataFrame(donors)
        self.donors_df.drop_duplicates(keep='last', inplace=True)
        self.charities_df = pd.DataFrame(charities)
        self.charities_df.drop_duplicates(keep='last', inplace=True)

        if auto==True:
            pass
        else:
            while True:
                fill_ids=input('Refresh orgs with missing ids? [yes/no]: ')

                if fill_ids == 'yes':
                    for df in [self.donors_df, self.charities_df]:
                        df.official_id = df.apply(lambda x: get_official_id(x.nm_uuid, self.token) if x.official_id is None else x.official_id, axis=1)
                    break
                elif fill_ids == 'no':
                    break
                else:
                    print('Enter yes/no')

        self.donors_df.columns=['official_id', 'uuid', 'branch_uuid', 'network_org_name', 'type']
        self.charities_df.columns=['official_id', 'nm_uuid', 'uuid', 'network_org_name', 'type']

        if cache_path is not None:

            donors_path = os.path.join(cache_path, 'donors_data.json')
            charities_path = os.path.join(cache_path, 'charities_data.json')

            with open(donors_path, 'w', encoding='utf-8') as f:
                donors_json = self.donors_df.apply(lambda x: x.to_dict(), axis=1).to_list()
                json.dump(donors_json, f, ensure_ascii=False, indent=4)

            with open(charities_path, 'w', encoding='utf-8') as f:
                charities_json = self.charities_df.apply(lambda x: x.to_dict(), axis=1).to_list()
                json.dump(charities_json, f, ensure_ascii=False, indent=4)

        return None



    def get_members_azure(self, fn_name):

        url=f'https://fv-webhooks.azurewebsites.net/api/get_members?code={fvdb.get_members_fn_key}'
        # members={}
        data = {"fn_name":fn_name}
        data=json.dumps(data)

        r=requests.post(url, data=data, headers= {"Content-Type":"application/json"})
        if r.status_code==200:
            members=json.loads(r.content)
        else:
            print(r, r.content)
            return r
        return members


    def refresh_members(self):
        '''test func - unused delete later
            current refresh on local only'''
        while True:
                update=input('Update Org data? [yes]/[no]: ')

                if update=='yes':
                    print("Org Refresh can be done with two types of criteria: \n")
                    print("1. num_days: Orgs created within n days will be refreshed and updated on the database \n")
                    print("2. names: comma seperated list of names to be refreshed on the database. (Names must match exactly) \n")
                    print("One of the filtering criteria must be used \n\n")
                    num_days = input("Enter Number of days (Integer) to filter new orgs:")
                    names = input("Enter comma-seperated ")

                    r=self.refresh_db_members(num_days, names)
                    if r.status_code==200:
                        print('Members refreshed')
                    else:
                        print(f'Error in member refresh: {r.status_code} : {r.content} \n')
                        print("Fall back to API Refresh")
                        pass
                        self.admin_api = FvUser(self.Email, self.PW, cache_path)
                if update=='no':
                    pass



    
    def refresh_db_members(num_days, names):
        names = ','.split(names)
        url=f'https://fv-webhooks.azurewebsites.net/api/update_members_by_criteria?code={fvdb.refresh_members_fn_key}'
        data_rm={"Names":names, "NumDays":num_days, "FoodNet":"FareShare"}

        data=json.dumps(data_rm)
        r=requests.post(url, data=data, headers= {"Content-Type":"application/json"})

        return r

        


class FvUser:
    def __init__(self, EMAIL, PW, cache_path, auto=False):
        '''authenticate and get token '''
        # print('******** Signing In with Admin Access ********')
        # if self.token:
        #     print('Already in session')
        # else:
    
        # token, user, branches, branch_ids = fv.authenticate_fv(fv.EMAIL, fv.PW)
        self.token=fv.authenticate_fv(EMAIL, PW)

        ## if token is None exit
        # if self.token is None:
        if not(isinstance(self.token, str)):
            print('exiting..')
            return None
            # sys.exit()


        ## set members data locations and refresh
        self.cache_path=cache_path
        print(self.cache_path)
        self.donors_data_file_location=os.path.join(self.cache_path,'donors_data.json')
        self.charities_data_file_location=os.path.join(self.cache_path,'charities_data.json')
        if auto==True:
            with open(self.charities_data_file_location, encoding='utf-8') as file:
                self.charities_data=json.load(file)
        
            with open(self.donors_data_file_location, encoding='utf-8') as file:
                self.donors_data=json.load(file)
        else:
            self.donors_data, self.charities_data=self.refresh_members()

        ## filter required cols in dfs
        ## get df containing nmuuid and official id only. (For now nmuuid and name, as all extids are null atm)
        donors_df=[]
        for donor in self.donors_data:
            # print(donor)
            # donors_df.append([self.donors_data['donor']['official_id'],self.donors_data[donor]['branch_name'],self.donors_data[donor]['uuid']])
            try:
                donors_df.append([donor['branch']['official_id'],donor['branch_name'],donor['uuid']])
            except KeyError:
                donors_df.append([donor['official_id'],donor['branch_name'],donor['uuid']])
            except TypeError:
                donors_df.append([donor['official_id'],donor['branch_name'],donor['uuid']])
        donors_df=pd.DataFrame(donors_df, columns=['official_id','name','uuid'])
        donors_df.official_id = donors_df.official_id.str.upper()
        print(len(donors_df))
        # print(donors_df)
        self.donors_df=donors_df
    
    
    
        ## get df containing nmuuid and official id only. (For now nmuuid and name, as all extids are null atm)
        charities_df=[]
        for charity in self.charities_data:
            # print(charity)
            # charities_df.append([self.charities_data[charity]['official_id'],self.charities_data[charity]['branch_name'],self.charities_data[charity]['branch_uuid']])
            try:
                charities_df.append([charity['branch']['official_id'],charity['branch_name'],charity['branch_uuid']])
            except KeyError:
                charities_df.append([charity['official_id'],charity['branch_name'],charity['uuid']])
        charities_df=pd.DataFrame(charities_df, columns=['official_id','name','uuid'])
        print(len(charities_df))
        charities_df['official_id'].loc[charities_df['official_id'].isna()]='N/A'
        self.charities_df=charities_df

        return None



    def refresh_members(self):
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
    
        ''' refresh org data '''
        if os.path.isfile(self.donors_data_file_location) and os.path.isfile(self.charities_data_file_location):
            ## if files exist
            while True:
                cached=input('Continue with saved Org data? [yes]/[no]: ')
                if cached.lower()=='yes':
                    with open(self.charities_data_file_location, encoding='utf-8') as file:
                        charities_data=json.load(file)
        
                    with open(self.donors_data_file_location, encoding='utf-8') as file:
                        donors_data=json.load(file)
                    print('Loaded saved cache files')
                    break
    
                elif cached.lower()=='no':
                    def get_date():
                        format='%Y-%M-%D'
                        while True:
                            date_string=input('\nEnter Refresh Start Date YYYY-MM-DD (Leave Blank for complete refresh): ')
                            try:
                                input_date=datetime.datetime.strptime(date_string, format='%Y-%M-%D')
                                break
                            except:
                                if date_string=='':
                                    input_date=None
                                    print('Complete refresh')
                                    break
                                else:
                                    print(f'{date_string} is not a valid date in format {format}')
                                    continue
                        return input_date

                    # start_date=get_date()
                    start_date=None

                    if start_date is None:
                        print('******** Refreshing Org Data ********')
                        donors_data, charities_data=fv.refresh_org_data(self.token, self.donors_data_file_location, self.charities_data_file_location)
                        ##write to file
                        break
                    else:
                        print(f'******** Refreshing Org Data from {start_date} ********')
                        donors_data, charities_data=fv.refresh_org_data(self.token, self.donors_data_file_location, self.charities_data_file_location, start_date=start_date)
                        ##append and deduplicate
                        ##write to file
                        break
                else:
                    print('please enter yer or no')
                    continue
    
        else:
            print('******** Refreshing Org Data ********')
            donors_data, charities_data=fv.refresh_org_data(self.token, self.donors_data_file_location, self.charities_data_file_location)
            ##write to file

        return donors_data, charities_data




class InputSchedule:
    def __init__(self, input_data, fvuser, time_format, exception_uploader=False, source='azure', conn=None):

        self.input_data=input_data
        self.fn_name=fvuser.fn_name

        ##olio exception uploader flag. If True:
            ## create column 'current primary' and 'Exception', set invite_to=olio_id in preprocess()
            ## set current_primary in set_window_data()
            ## run set_exception in preprocess(), filter and create exception_df
            ## print exception logs and message in main flow with InputSchedule.Exception_df
        self.exception_uploader=exception_uploader
        self.olio_id='OLIO:0011p00002gwllUAAQ'
        self.exception_df=None
        
        if self.exception_uploader:
            print("setting olio invite")
            self.input_data.loc[:, 'invite_to']=self.olio_id
            self.input_data.loc[:,'add_primary']='add'

        ## check if donation/window cols provided and set flags to T/F
        donation_window_cols=['Donation Starts At', 'Donation Ends At']
        collection_window_cols=['Collection Starts At', 'Collection Ends At', 'Day Of Week Start', 'Day Of Week End', 'invite_to', 'remove_primary', 'uninvite', 'add_waitlist', 'remove_waitlist' ]
        self.timecols=['Donation Starts At', 'Donation Ends At', 'Collection Starts At', 'Collection Ends At']
        input_cols=input_data.columns.to_list()
        self.update_dw = any(x in input_cols for x in donation_window_cols)
        self.update_cw = any(x in input_cols for x in collection_window_cols)

        ## create empty timecols if not in input
        ## required arg for api setting window

        for col in self.timecols:
            if not(col in input_data.columns):
                input_data.loc[:,col]=None

        for col in ['max_collectors', 'is_active']:
            if not(col in input_data.columns):
                input_data.loc[:,col]=None




        ''' flag and filter out rejected records '''
        input_validated, flag_cols=self.validate_input(input_data, fvuser.donors_df, fvuser.charities_df, time_format)
        valid_records=input_validated.loc[ (input_validated['Time Format Error'].isna()) & (input_validated['DonorNotFoundFlag'].isna()) & (input_validated['ValidDayOfWeek'].isna()) ,:]
        self.input_validated=input_validated
        self.flag_cols=flag_cols
        self.valid_records=valid_records


        ### init empty variables to store upload log dataframes
        self.upload_logs=None
        self.invitation_accept_response=None

        ## schedule source db/api
        sources=['db', 'api', 'azure']
        # self.source=sources[2]
        self.source=source

        self.conn = conn

        return None




    ''' validate input:
        1. get uuids for donors and chars, if does not exist, set flags
        2. valid timestring in time cols
        3. dow in [0,6]
        4. todo: validate start time < end time set end day = start day + 1. Currently handled in collection window builder as temp fix
    '''



    def validate_input(self,input_data, donors_df, charities_df, time_format):


        ## check that all donor official ids exist on FV
        input_data=validate_donor_exists(input_data, donors_df)

        ## validate that charity exists for all char cols in input
        flag_cols=[]
        charity_cols=['add_waitlist', 'remove_waitlist', 'invite_to']
        for col in charity_cols:
            if col in input_data.columns:
                flag_cols.append(col+'_CharityNotFound')
                input_data[col]=input_data[col].fillna('').astype('str')
                input_data=validate_charity_exists(input_data, charities_df, col)
        input_data['CharityNotFoundFlag']=''
        for flag_col in flag_cols:
            input_data['CharityNotFoundFlag']=input_data['CharityNotFoundFlag'].astype('str')+input_data[flag_col]+'\n'
    
        ## check that time formats are valid and start<end
        input_data=validateTimeFormat(input_data, time_format)
    
        print('******** Input Data Validation Summary ********')
        print('Total Records: ', len(input_data))
        print('Valid Records: ', len(input_data.loc[input_data['Time Format Error'].isna() & input_data['DonorNotFoundFlag'].isna() ,:]))
        print('Time Format Error: ', input_data['Time Format Error'].notna().sum(), ' rows (Rejected)')
        print('Donor Not Found: ', input_data['DonorNotFoundFlag'].notna().sum(), ' rows (Rejected)')
        # print('Charity Not Found: ', input_data['CharityNotFoundFlag'].notna().sum(), ' rows (Will run if other inputs are valid')

        return input_data, flag_cols

    
    '''
    create required cols, set donation/collection window data and set in valid_records'''


    def preprocess(self, token):
        valid_records=self.valid_records
        ##create window id cols:
        valid_records.loc[:,'DonationWindowId']=None
        valid_records.loc[:,'CollectionWindowId']=None
        valid_records.loc[:,'CollectionWindow']=None
        valid_records.loc[:,'current_primary']=None
        valid_records.loc[:,'invitation_scheduled_activation_date']=None
        valid_records.loc[:,'DonationWindows']=None
        valid_records.loc[:,'cw_data']=None
        # valid_records.loc[:,'payload']=None
        # valid_records.loc[:,'info']=None
        valid_records.loc[:,'fatal']=None
        valid_records.CollectionWindow=valid_records.CollectionWindow.astype(object)
        valid_records.DonationWindows=valid_records.DonationWindows.astype(object)
        valid_records.cw_data=valid_records.cw_data.astype(object)
        if self.exception_uploader:
            print('Olio Exception Uploader Procedure')
            valid_records.loc[:,'current_primary']=None
            valid_records.loc[:,'Exception']=None
            valid_records.loc[:,'invite_to']=self.olio_id

        ## create time_fv columns to set current window time if not provided in input
        # timecols=['Donation Starts At', 'Donation Ends At', 'Collection Starts At', 'Collection Ends At']
        timecols=self.timecols
        cols=timecols + ['max_collectors', 'is_active']
        # for col in timecols.extend(['max_collectors', 'is_active']):
        for col in cols:
            valid_records.loc[:,col+'_fv']=None


        ## add required collection window payload cols if do not exist
        collection_payload_cols=['invite_to', 'uninvite', 'remove_primary', 'add_waitlist', 'remove_waitlist', 'invitation_starts_at']
        ## if any collection payload cols in input:
        update_collection= any(item in collection_payload_cols for item in valid_records.columns.to_list())
        for col in collection_payload_cols:
            if not(col in valid_records.columns.to_list()):
                if col in ['remove_primary', 'uninvite', 'invitation_starts_at']:
                    valid_records.loc[:,col]=None
                else:
                    valid_records.loc[:,'uuid_'+col]=None

        self.valid_records=valid_records

        if self.source=='db':

            uuids_list=valid_records.uuid.to_list()
            self.windows = fvdb.get_uploader_windows(uuids_list, self.conn, donation_windows=True, collection_windows=True)

        elif self.source=='azure':
            ids = list(valid_records.uuid.unique())
            self.windows = fvdb.get_uploader_windows_azure(ids)
                    

        ## todo set_window_data should return df instead of set on df in each iter
        # if update_collection:
        print('******** Fetching Current Schedules ********')
        print('This may take a while (~2-3 s per donor in the input)) ')
        self.valid_records.groupby('Key').apply(lambda x: self.set_window_data(x, token))
        print('******** Schedules Imported. Calculating Updates ********')
        pl=self.valid_records.apply(lambda x: self.get_payload(x), axis=1)
        pldf=pd.DataFrame(pl.to_list(), columns=['info','payload'])
        self.valid_records=pd.concat([self.valid_records, pldf], axis=1)
            ## set_window_data sets vals on self.valid_records (mistake) requiring variable switch to call the function
        # else:
        #     print('******** No Collection Assingment Updates ********')
        #     self.valid_records.loc[:,'info']=None
        #     self.valid_records.loc[:,'payload']=None
            ## set times to window times if input times is null. if both are null, raise to user
        valid_records=self.valid_records
        if self.update_dw:
            for col in ['Donation Starts At', 'Donation Ends At']:
                for i in range(len(valid_records)):
                    if pd.isnull(valid_records[col].iloc[i]) and pd.isnull(valid_records[col+'_fv'].iloc[i]):
                        valid_records['info'].iloc[i] = str(valid_records['info'].iloc[i]) + '\n Must provide start/end time if Window does not exist'
                    elif pd.isnull(valid_records[col].iloc[i]):
                        valid_records[col].iloc[i]=valid_records[col+'_fv'].iloc[i]
        ## will cause warning if exception=True and window does not exist
            # set is_active
            for i in range(len(valid_records)):
                if pd.isnull(valid_records['is_active'].iloc[i]) and pd.isnull(valid_records['is_active_fv'].iloc[i]):
                    valid_records['is_active'].iloc[i]=1
                elif pd.isnull(valid_records['is_active'].iloc[i]):
                    valid_records['is_active'].iloc[i]=valid_records['is_active_fv'].iloc[i]
            # self.valid_records=valid_records



        if self.update_cw:
            for col in ['Collection Starts At', 'Collection Ends At']:
                for i in range(len(valid_records)):
                    if pd.isnull(valid_records[col].iloc[i]) and pd.isnull(valid_records[col+'_fv'].iloc[i]):
                        valid_records['info'].iloc[i] = str(valid_records['info'].iloc[i]) + '\n Must provide start/end time if Window does not exist'
                    elif pd.isnull(valid_records[col].iloc[i]):
                        valid_records[col].iloc[i]=valid_records[col+'_fv'].iloc[i]
            
            # calc max_collect
            for i in range(len(valid_records)):
                if pd.isnull(valid_records['max_collectors'].iloc[i]) and pd.isnull(valid_records['max_collectors_fv'].iloc[i]):
                    valid_records['max_collectors'].iloc[i]=1
                elif pd.isnull(valid_records['max_collectors'].iloc[i]):
                    valid_records['max_collectors'].iloc[i]=valid_records['max_collectors_fv'].iloc[i]

            ## get posting day for all rows
            posting_window_df = pd.DataFrame(valid_records.apply(lambda x: self.get_posting_window(x), axis=1).to_list(), columns=['posting_window_id', 'posting_window', 'posting_window_info'])
            valid_records=pd.concat([valid_records, posting_window_df], axis=1)
            # valid_records.loc[:,'posting_status'] = valid_records.apply(lambda x: self.posting_window_final_status(x), axis=1)

        self.valid_records=valid_records


        ## if exception = True set exception columns
        if self.exception_uploader:
            ## check current primary:
            ## if olio: already assignef
            ## if empty: update
            ## else: exception
            # print(valid_records.current_primary)
            valid_records.loc[:, 'Exception']='Exception'
            valid_records.Exception.loc[ pd.isnull(valid_records["current_primary"]) | (valid_records['current_primary']=='') ] ='Update'
            valid_records.Exception.loc[ valid_records['current_primary']==self.olio_id ] ='Assigned to Olio'

            ## set exception_df
            exception_cols=self.input_data.columns.to_list()
            exception_cols.extend(['Exception'])
            self.exception_df=valid_records.loc[:, exception_cols]

            ## filter valid_records to get records to be updated
            self.valid_records=self.valid_records.loc[self.valid_records.Exception=='Update', :]


        return None
    
    def get_posting_window(self, x):
        ''' given collection window and list of donation windows, 
            for collection window dow i, calculate previous nearest donation window
            set is_active as per add/remove primary in collection update input
            return donation_window_id and donation window update payload
        '''
        collection_day=int(x.DayOfWeek)
        collection_start=x['Collection Starts At']
        # print('Collection Day: ', collection_day)
        # print(f'Prev day: {(collection_day-1)%7}')
        if pd.isnull(collection_start) :
            print('Invalid Collection Window Start Time')
            return [None, None, 'Invalid Collection Window Start Time']
        # no windows on both days
        if len(x.DonationWindows[int(collection_day)])==0 and len(x.DonationWindows[int((collection_day-1)%7)])==0:
            print('No Donation Window')
            return [None, None, 'No Donation Window']
        # windows on both days
        elif len(x.DonationWindows[int(collection_day)])>0 and len(x.DonationWindows[(int(collection_day-1)%7)])>0:
            ## loop through latest to earlierst to handle multiple dws
            dws_dow=x.DonationWindows[collection_day]
            dws_dow=sorted([dw for dw in dws_dow if dw['starts_at']<collection_start], key=lambda k: k['starts_at'])
            dw_prev=x.DonationWindows[(collection_day-1)%7][-1]

            if len(dws_dow)>0:
                dw_dow=dws_dow[-1]
                donation_start=dw_dow['starts_at']
                if donation_start < collection_start:
                    # print('\n\nSame day posting ')
                    return [dw_dow['uuid'], dw_dow, None]
                else:
                    return [dw_prev['uuid'], dw_prev, None]
            else: return [dw_prev['uuid'], dw_prev, None]
        # no windows on cday, window present on cday-1
        elif len(x.DonationWindows[collection_day])==0:
            dw_prev=x.DonationWindows[int((collection_day-1)%7)][0]
            return [dw_prev['uuid'], dw_prev, None]
        # no window on cday-1, present on cday
        else:
            dw_dow=x.DonationWindows[collection_day][0]
            donation_start=dw_dow['starts_at']
            if donation_start < collection_start:
                return [dw_dow['uuid'], dw_dow, None]
            else:
                print('No Donation Window')
                return [None, None, 'No Donation Window']


    def posting_window_final_status(self, x):
        '''input row of valid_records, find if another collection window is associated with the current posting window and if it has a primary
        if yes, do not deactivate posting window
        To account for multi collector scenarios
        Call only where posting window is found (posting_info is null) and remove_primary=='remove' '''
        posting_start=x.posting_window['starts_at']
        posting_start_day=x.posting_window['day_of_week']
        # same_day_window=[window for window in x.DonationWindows if window['day_of_week']==x.posting_window['day_of_week'] and window['starts_at']>posting_start]
        # next_day_window=[window for window in x.DonationWindows if window['day_of_week']==((x.posting_window['day_of_week'])%7)+1 and window['starts_at']<posting_start]
        same_day_window=[window for window in x.DonationWindows[posting_start_day] if window['starts_at']>posting_start]
        next_day_window=[window for window in x.DonationWindows[((posting_start_day%7)+1)%7] if window['day_of_week']==(((x.posting_window['day_of_week'])%7)+1)%7 and window['starts_at']<posting_start]

        if len(same_day_window)>0:
            next_window=same_day_window[0]
            end_day=next_window['day_of_week']
            end_time=next_window['starts_at']
        elif len(next_day_window)>0:
            next_window=next_day_window[0]
            end_day=next_window['day_of_week']
            end_time=next_window['starts_at']
        else:
            end_day=(x.posting_window['day_of_week']%7)+1
            end_time=posting_start

        active_cws = [cw for cw in x.cw_data if (cw['day_of_week_start']==posting_start_day and cw['starts_at']>posting_start and cw['starts_at']<end_time) or (cw['day_of_week_start']==end_day and cw['starts_at']<end_time)]
        active_cws = [cw for cw in active_cws if cw['uuid']!=x.CollectionWindowId and cw['primary_uuid'] is not None]
        
        if len(active_cws)>0:
            posting_window_status=1
        else:
            posting_window_status=0
        
        return posting_window_status
        



        

        
        


    def get_payload(self, x):
        ## for each row of valid records, get payload
        if x.CollectionWindow is None:

             payload_builder={
            'add_waitlist':x['uuid_add_waitlist'], 'invite_to':x['uuid_invite_to'], 'invitation_starts_at':x['invitation_starts_at']}
            #  for k,v in payload_builder.items():
            #      # payload_builder[k]=groupbydf.iloc[i,:][k]
            #      payload_builder[k]=x['uuid_'+k]
     
             payload, info=fv.collection_window_payload_builder(collection_window=None,
                                                                remove_primary=None,
                                                                remove_waitlist=None,
                                                                uninvite=None,
                                                                add_waitlist=payload_builder['add_waitlist'],
                                                                invite_to=payload_builder['invite_to'],
                                                                invitation_starts_at=payload_builder['invitation_starts_at'])
        else:
             ##set optioal args for colletion_payload_builder, if argname in df.columns
             payload_builder={
            'remove_primary':x['remove_primary'], 'uninvite':x['uninvite'],
            'add_waitlist':x['uuid_add_waitlist'], 'remove_waitlist':x['uuid_remove_waitlist'],
            'invite_to':x['uuid_invite_to'], 'invitation_starts_at':x['invitation_starts_at']}
            #  print(payload_builder)
 
             payload, info=fv.collection_window_payload_builder(collection_window=x.CollectionWindow,
                                                                remove_primary=payload_builder['remove_primary'],
                                                                remove_waitlist=payload_builder['remove_waitlist'],
                                                                uninvite=payload_builder['uninvite'],
                                                                add_waitlist=payload_builder['add_waitlist'],
                                                                invite_to=payload_builder['invite_to'],
                                                                invitation_starts_at=payload_builder['invitation_starts_at'])


        return [info,payload]

    ## to output of input_validation: groupby('donor').apply df object to this function to return (and set) window ids using donorid and dow
    ## \\todo add flag for when len(windows)>1
    def set_window_data(self, groupbydf, token):
        ## this is a groupby branch df, so can pick first val
        branch_uuid=groupbydf['uuid'].iloc[0]
        print(branch_uuid)
        
        if self.source in ['db', 'azure']:
            try:
                windows=self.windows[branch_uuid]
            except:
                windows={}
                windows['donation_windows']=[[],[],[],[],[],[],[]]
                windows['collection_windows']=[[],[],[],[],[],[],[]]
        else:
            print(groupbydf)
            windows=fv.get_donor_windows(token, branch_uuid)
            # temp dumb sleep
            time.sleep(0.7)
        ## if upadate donation_window is True:
        if self.update_dw:
            # donation_windows=fv.get_donor_donation_windows(token, branch_uuid)
            donation_windows=windows['donation_windows']

            ## if windows is response object, i.e. get_window function error, set info and return
            if type(donation_windows)==requests.Response:
                error_str='Error getting Donation Window Response-'+str(donation_windows.status_code)
                self.valid_records.loc[(self.valid_records.uuid==branch_uuid),'fatal']=str(self.valid_records.loc[(self.valid_records.uuid==branch_uuid),'fatal'])+' ; '+error_str
    
            else:
                ##loop through all rows per donor
                for i in range(len(groupbydf)):
                    dow=groupbydf.iloc[i,:]['DayOfWeek']
                    ## if there are more than one donation/collection windows per day, use the first
            
                    if len(donation_windows[dow])>0:
                        donation_window=donation_windows[dow][0]
                        donation_window_uuid=donation_window['uuid']
                        donation_starts=donation_window['starts_at']
                        donation_ends=donation_window['ends_at']
                        is_active=donation_window['is_active']
                    else: 
                        donation_window_uuid='Not Found'
                        donation_starts=None
                        donation_ends=None
        
                    ## set values in valid_records.
                    ## \\todo need to set these correctly

                    self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'DonationWindowId']=donation_window_uuid
                    self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'Donation Starts At_fv']=donation_starts
                    self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'Donation Ends At_fv']=donation_ends
                    self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'is_active_fv']=is_active

        ## if update collection window is True:
        if self.update_cw:
            # collection_windows=fv.get_donor_collection_windows(token, branch_uuid)
            collection_windows=windows['collection_windows']
            

            ## if windows is response object, i.e. get_window function error, set info and return
            if type(collection_windows)==requests.Response:
                error_str='Error getting Donation Window Response-'+str(collection_windows.status_code)
                self.valid_records.loc[(self.valid_records.uuid==branch_uuid),'fatal']=str(self.valid_records.loc[(self.valid_records.uuid==branch_uuid),'fatal'])+' ; '+error_str

            else:
                cw_data=[cw for sublist in collection_windows for cw in sublist]
                cws=[{'uuid':cw['uuid'], 'starts_at':cw['starts_at'], 'ends_at':cw['ends_at'], 'day_of_week_start':cw['day_of_week_start'], 'primary_uuid':cw['primary_charity_branch']['uuid']} 
                for cw in cw_data if cw['primary_charity_branch'] is not None] 

                cw_array = np.empty(1, dtype=object)
                cw_array[0]=cws
                # print(cw_array)
                # self.valid_records.loc[(self.valid_records.uuid==branch_uuid),'cw_data']=cw_array
                ##loop through all rows per donor
                for i in range(len(groupbydf)):
                    dow=groupbydf.iloc[i,:]['DayOfWeek']
                    if len(collection_windows[dow])>0:
                        collection_window=collection_windows[dow][0]
                        collection_window_uuid=collection_window['uuid']
                        collection_starts=collection_window['starts_at']
                        collection_ends=collection_window['ends_at']
                        max_collectors=collection_window['max_collectors']
                        if collection_window['primary_charity_branch']:
                            self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'current_primary']=collection_window['primary_charity_branch']['charity_branch']['official_id']
                        if collection_window['invited_branch']:
                            invitation_scheduled_activation_date=collection_window['invited_branch']['scheduled_activation_date']
                        else:
                            invitation_scheduled_activation_date=None


                        if self.exception_uploader:
                            ## if primary exists, set id else set None
    
                            if collection_window['primary_charity_branch']:
                                self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'current_primary']=collection_window['primary_charity_branch']['charity_branch']['official_id']
                            else:
                                self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'current_primary']=None



                    else:
                        ## if window does not already exist, get add_wl/to_invite, other args to None
                        ## windowuuid='Not Found': used by fv.update_window to POST
                        collection_window=None
                        collection_window_uuid='Not Found'
                        collection_starts=None
                        collection_ends=None
                        max_collectors=None
                        invitation_scheduled_activation_date=None

    
                    self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'CollectionWindowId']=collection_window_uuid
                    self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'CollectionWindow']=[collection_window] * len(self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'CollectionWindow']) ## todo quick fix to change later
                    ## set dw to update posting window is_active
                    # self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'DonationWindows']=[windows['donation_windows']] * len(self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'DonationWindows'])
                    dw=np.empty(1, dtype=object)
                    # dw_list = [dw for sublist in windows['donation_windows'] for dw in sublist]
                    dw_list = windows['donation_windows']
                    dw[0]=dw_list
                    # dw=json.load(windows['donation_windows'])
                    self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'DonationWindows']=[dw] * len(self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'DonationWindows'])
                    self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'cw_data']=[cw_array] * len(self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'cw_data'])
                    # self.valid_records.at[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'DonationWindows']=np.array([windows['donation_windows']], dtype=object) * len(self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'DonationWindows'])
                    # print(collection_window_uuid)
                    # print(payload)
                    # self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'payload']=[payload]
                    # self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'info']=info
            
                    self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'Collection Starts At_fv']=collection_starts
            
                    self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'Collection Ends At_fv']=collection_ends

                    self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'max_collectors_fv']=max_collectors
                    self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'invitation_scheduled_activation_date']=invitation_scheduled_activation_date

                    # if self.exception_uploader:
                    #     ## if primary exists, set id else set None

                    #     if collection_window['primary_charity_branch']:
                    #         self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'current_primary']=collection_window['primary_charity_branch']['charity_branch']['official_id']
                    #     else:
                    #         self.valid_records.loc[(self.valid_records.uuid==branch_uuid) & (self.valid_records.DayOfWeek==dow),'current_primary']=None
        return None

    def dw_activation(self, valid_record, token, fn, n_days):
        ''' activate/deactivate dws
            For fs: exclude Tesco UK
            For fc: activate for invitations activating in next n days (normally 7) (can be done for both fns anyway?)
            Returns list [store id, dow, update respnse]; transform to series -> df
        '''
        if fn == 'fs':
            ## if posting window not found, error recorded in posting window info
            if not(pd.isnull(valid_record.current_primary)) and pd.isnull(valid_record.posting_window_info):
                print(valid_record.current_primary, "  ", valid_record.posting_window_info)
                print(f'Activation DW for {valid_record.Key} : {valid_record.DayOfWeek}')
                posting_window_pl=valid_record.posting_window
                posting_is_active=posting_window_pl['is_active']
                if posting_is_active==0:
                    pw_r=fv.update_donation_window(token,
                                            nm_uuid=valid_record.uuid, window_uuid=valid_record.posting_window_id,
                                            day_of_week=posting_window_pl['day_of_week'],
                                            starts_at=posting_window_pl['starts_at'],
                                            ends_at=posting_window_pl['ends_at'],
                                            is_active=1)
                else: pw_r='Already Active'
            
            elif pd.isnull(valid_record.current_primary) and pd.isnull(valid_record.posting_window_info):
                print(f'Deactivation DW for {valid_record.Key} : {valid_record.DayOfWeek}')
                print(valid_record.current_primary, "  ", valid_record.posting_window_info)
                posting_window_pl=valid_record.posting_window
                posting_is_active=posting_window_pl['is_active']
                if str(posting_is_active)=='1':
                    pw_r=fv.update_donation_window(token,
                                            nm_uuid=valid_record.uuid, window_uuid=valid_record.posting_window_id,
                                            day_of_week=posting_window_pl['day_of_week'],
                                            starts_at=posting_window_pl['starts_at'],
                                            ends_at=posting_window_pl['ends_at'],
                                            is_active=0)
                else: pw_r='Already Inactive'
            
            else: pw_r=None
        
        elif fn == 'fc':
            ## if posting window not found, error recorded in posting window info
            if pd.isnull(valid_record.invitation_scheduled_activation_date):
                scheduled_activation=False
            else:
                try:
                    scheduled_activation = ( datetime.datetime.strptime(valid_record.invitation_scheduled_activation_date) >= datetime.datetime.today() + datetime.timedelta(days=n_days) )
                except:
                    scheduled_activation = False
                
            if (not(pd.isnull(valid_record.current_primary)) or scheduled_activation) and pd.isnull(valid_record.posting_window_info):
                print(valid_record.current_primary, "  ", valid_record.posting_window_info)
                print(f'Activation DW for {valid_record.Key} : {valid_record.DayOfWeek}')
                posting_window_pl=valid_record.posting_window
                posting_is_active=posting_window_pl['is_active']
                if str(posting_is_active)=='0':
                    pw_r=fv.update_donation_window(token,
                                            nm_uuid=valid_record.uuid, window_uuid=valid_record.posting_window_id,
                                            day_of_week=posting_window_pl['day_of_week'],
                                            starts_at=posting_window_pl['starts_at'],
                                            ends_at=posting_window_pl['ends_at'],
                                            is_active=1)
                    # pw_r='Activate'
                else: pw_r='Already Active'
            
            elif pd.isnull(valid_record.current_primary) and not(scheduled_activation) and pd.isnull(valid_record.posting_window_info):
                print(f'Deactivation DW for {valid_record.Key} : {valid_record.DayOfWeek}')
                print(valid_record.current_primary, "  ", valid_record.posting_window_info)
                posting_window_pl=valid_record.posting_window
                posting_is_active=posting_window_pl['is_active']
                if posting_is_active==1:
                    pw_r=fv.update_donation_window(token,
                                            nm_uuid=valid_record.uuid, window_uuid=valid_record.posting_window_id,
                                            day_of_week=posting_window_pl['day_of_week'],
                                            starts_at=posting_window_pl['starts_at'],
                                            ends_at=posting_window_pl['ends_at'],
                                            is_active=0)
                    # pw_r='Deactivate'
                else: pw_r='Already Inactive'
            
            else: pw_r=None

        return [valid_record.Key, valid_record.DayOfWeek, pw_r]


    def csv_to_tfv(self,valid_record, token):

        ''' update valid records after user confirmation '''
        ## call fv.update_donation/collection on every row

        if self.update_dw:
            dw_r=fv.update_donation_window(token,
                                        nm_uuid=valid_record.uuid, window_uuid=valid_record.DonationWindowId,
                                        day_of_week=valid_record.DayOfWeek+1,
                                        starts_at=valid_record['Donation Starts At'],
                                        ends_at=valid_record['Donation Ends At'],
                                        is_active=valid_record['is_active'])
            print(dw_r.content) ## and log
        else:
            
            dw_r=None

    
        if self.update_cw:

            ## 2804 temp fix to handle collection windows across more than one day, i.e. continues past midnight
            # should be moved to preprocess and added to collection_window logs?
            try:
                start_time=datetime.datetime.strptime(valid_record['Collection Starts At'], '%H:%M:%S') ## there is a _parsed col already in valid?
                end_time=datetime.datetime.strptime(valid_record['Collection Ends At'], '%H:%M:%S')
                dow_start=valid_record.DayOfWeek+1

                if end_time < start_time:
                    dow_end=dow_start+1
                else:
                    dow_end=dow_start
                
            except:
                print(sys.exc_info())
                ## this would have been caught in validate()
                print('Error parsing time; setting start_day = end_day')
                dow_end=valid_record.DayOfWeek

            cw_r=fv.update_collection_window_timings(token,
                                                     nm_uuid=valid_record.uuid, window_uuid=valid_record.CollectionWindowId,
                                                     day_of_week_start=valid_record.DayOfWeek+1,day_of_week_end=dow_end,
                                                     starts_at=valid_record['Collection Starts At'],
                                                     ends_at=valid_record['Collection Ends At'],
                                                     added_waitlist=valid_record['payload']['added_waitlist'],
                                                     removed_waitlist=valid_record['payload']['remove_waitlist'],
                                                     uninvited=valid_record['payload']['uninvited'],
                                                     to_invite=valid_record['payload']['to_invite'],
                                                     removed_primary=valid_record['payload']['removed_primary'],
                                                     max_collectors=valid_record.max_collectors)
            print(cw_r.content) ## and log
            if cw_r is None or cw_r.status_code!=200:
                print('Update Collection Window failed. Skipping Posting Window Activation/Deactivation')
                pw_r='Update Collection Window failed. Skipping Posting Window Activation/Deactivation'
                return [valid_record.Key, valid_record.DayOfWeek, dw_r, cw_r, pw_r]
        else:
            cw_r=None
            # pw_r=None
            # return [valid_record.Key, valid_record.DayOfWeek, dw_r, cw_r, pw_r]

        
        
        # activate/deactivate
        # if not tesco
        if not(valid_record.Key.startswith('TESCO') and self.fn_name=='fareshare'):
            if 'add_primary' in valid_record.index:
                ## if posting window not found, error recorded in posting window info
                if valid_record.add_primary=='add' and pd.isnull(valid_record.posting_window_info):
                    posting_window_pl=valid_record.posting_window
                    posting_is_active=posting_window_pl['is_active']
                    if posting_is_active==0:
                        pw_r=fv.update_donation_window(token,
                                                nm_uuid=valid_record.uuid, window_uuid=valid_record.posting_window_id,
                                                day_of_week=posting_window_pl['day_of_week'],
                                                starts_at=posting_window_pl['starts_at'],
                                                ends_at=posting_window_pl['ends_at'],
                                                is_active=1)
                    else: pw_r='Already Active'
                else: pw_r=None
            
            elif 'remove_primary' in valid_record.index: 
                if valid_record.remove_primary=='remove' and pd.isnull(valid_record.posting_window_info):
                    posting_window_pl=valid_record.posting_window
                    posting_is_active=posting_window_pl['is_active']
                    if posting_is_active==1:
                        pw_r=fv.update_donation_window(token,
                                                nm_uuid=valid_record.uuid, window_uuid=valid_record.posting_window_id,
                                                day_of_week=posting_window_pl['day_of_week'],
                                                starts_at=posting_window_pl['starts_at'],
                                                ends_at=posting_window_pl['ends_at'],
                                                is_active=0)
                    else: pw_r='Already Inactive'
                else: pw_r=None
            
            else: pw_r=None
        
        else: pw_r = 'is Tesco'

    
    
        # print(valid_record.Key, valid_record.DayOfWeek, dw_r, cw_r, sep='\t')
    
        # upload_log=[]
        # upload_log.append([valid_record.Key, valid_record.DayOfWeek, dw_r, cw_r])
        time.sleep(1)

        return [valid_record.Key, valid_record.DayOfWeek, dw_r, cw_r, pw_r]
        # return upload_log

    ## update if any valid records
    def upload_schedules(self, token):
        ## if no valid records exit
        valid_records=self.valid_records.loc[pd.isnull(self.valid_records.fatal),:]
        if len(valid_records)==0:
            print('\n No valid records to continue with updates. Exiting update schedules...')
            return None
        upload_logs=valid_records.apply(lambda x: self.csv_to_tfv(x, token), axis=1)
        self.upload_logs=pd.DataFrame(upload_logs.to_list(), columns=['Key', 'DayOfWeek', 'dw_r', 'cw_r', 'pw_r'])

        return self.upload_logs

    def set_posting_activation(self, token, fn, n_days):
        valid_records=self.valid_records.loc[pd.isnull(self.valid_records.fatal),:]
        if len(valid_records)==0:
            print('\n No valid records to continue with updates. Exiting update schedules...')
            return None
        # upload_logs=valid_records.apply(lambda x: self.dw_activation(x, token) if not(x.Key.startswith('TESCO')) else [x.Key, x.DayOfWeek, 'isTesco'], axis=1)
        upload_logs=valid_records.apply(lambda x: self.dw_activation(x, token, fn, n_days) , axis=1)
        upload_logs=pd.DataFrame(upload_logs.to_list(), columns=['Key', 'DayOfWeek', 'pw_r'])

        return upload_logs


    ''' filter invitation records, get invitation uuids, grp by charity, sign in and accept '''

    def accept_invitations(self, token, creds):
        valid_records=self.valid_records
        ## for each donor get schedules and set invitation uuid for dow in list
        ## filter df where uuid_invite_to is found and add_primary is given
        self.invitation_records = valid_records.loc[ (pd.notna(valid_records.uuid_invite_to)) & (valid_records.add_primary=='add'),:]
        if len(self.invitation_records)==0:
            print('\n\n******** No Invitations ********')
            return None
        else:
    
            self.invitation_records.loc[:,'Invitation Id']=None
            print('\n\n\n******** Getting Invitations ********')
            if self.source == 'db':
                self.windows = fvdb.get_uploader_windows(self.invitation_records.uuid.to_list(), self.conn)
            elif self.source == 'azure':
                ids = list(self.invitation_records.uuid.unique())
                self.windows = fvdb.get_uploader_windows_azure(ids)
                    
            self.invitation_records.groupby('Key').apply(lambda x: self.get_invitation_id(x,token) )
            dsf=input('Continue with accepts')
            print('\n\n\n******** Accepting Invitations ********')
            print( 'Found ', len(self.invitation_records['Invitation Id']), ' Invitation IDs')

            creds_key='official_id'
            input_key='invite_to'
        
            # self.invitation_records=self.invitation_records.merge(creds, how='left', left_on=input_key, right_on=creds_key)

            invitation_accept_response=self.invitation_records.groupby('invite_to').apply(lambda x: self.signin_and_accept(x))
            invitation_accept_response=invitation_accept_response.reset_index()
            return invitation_accept_response

    def get_invitation_from_response(self, collection_windows, dow):
        if len(collection_windows[dow])>0:
            collection_window=collection_windows[dow][0]
            if collection_window['invited_branch']:
                invitation_id=collection_window['invited_branch']['uuid']
                info=None
            else:
                invitation_id=None
                info='invitation if not found'
        else:
            invitation_id=None
            info='Collection Window Not Found'
        return invitation_id, info

    def get_invitation_id(self, groupbydf, token):
        branch_uuid=groupbydf['uuid'].iloc[0]
        # if self.source == 'db':
        #     collection_windows = self.windows[branch_uuid]['collection_windows']
        # else:
        #     collection_windows=fv.get_donor_collection_windows(token, branch_uuid)
        #     time.sleep(0.2)
        collection_windows=fv.get_donor_collection_windows(token, branch_uuid)
        time.sleep(0.2)

        for i in range(len(groupbydf)):
            dow=groupbydf.iloc[i,:]['DayOfWeek']

            invitation_id, info = self.get_invitation_from_response(collection_windows, dow)

            self.invitation_records.loc[(self.invitation_records.uuid==branch_uuid) & (self.invitation_records.DayOfWeek==dow),'Invitation Id']=invitation_id
        return None
    
    def get_invitation_id_db(self, groupbydf, token):
        branch_uuid=groupbydf['uuid'].iloc[0]
        # collection_windows=fv.get_donor_collection_windows(token, branch_uuid)
        # time.sleep(0.2)
        collection_windows = self.windows['branch_uuid']
        for i in range(len(groupbydf)):
            dow=groupbydf.iloc[i,:]['DayOfWeek']

            invitation_id, info = self.get_invitation_from_response(collection_windows, dow)

            self.invitation_records.loc[(self.invitation_records.uuid==branch_uuid) & (self.invitation_records.DayOfWeek==dow),'Invitation Id']=invitation_id
        
        inv_series = self.invitation_records.loc[self.invitation_records.uuid==branch_uuid,'Invitation Id']
        ## if any inv not found, confirm with apis
        if inv_series.isnull().values.any():
            self.get_invitation_id(self, groupbydf, token)


        return None



    ## groupby invite_to, sign in and accept all invitations for current charity
    def signin_and_accept(self,groupbydf):
        r_log=[]
        # email=groupbydf.email.iloc[0]
        # password=groupbydf.password.iloc[0]
        invite_to_id=str(groupbydf.invite_to.iloc[0]).replace(':','')
        invite_to_char=groupbydf.invite_to.iloc[0]

        ## set email- separate from pattern for olio
        if groupbydf.invite_to.iloc[0]==self.olio_id:
            email='volunteer+foodcloud@olioex.com'
        else:
            email='fvsupport+'+invite_to_id+'@foodcloud.ie'
        # email='volunteer+foodcloud@olioex.com'
        password='F00D1ver$e'
        print(email)
        charity_token = fv.authenticate_fv(email, password)
        # if type(charity_token)=='requests.Response':
        if isinstance(charity_token, requests.Response):
            r_log.append([invite_to_char, None, charity_token.status_code, charity_token.content])

        elif type(charity_token)==str:
            for i in range(len(groupbydf)):
                invitation_uuid=groupbydf['Invitation Id'].iloc[i]
                if invitation_uuid:
                    r=fv.accept_invitation(charity_token, invitation_uuid)
                    time.sleep(0.2)
                    print(invite_to_char, groupbydf.Key.iloc[i], r)
                    if r.status_code!=200:
                        content=str(r.content)
                    else:
                        content='Success'
                    remove_chars=['"', "'", '\n','\t']
                    for ch in remove_chars:
                        content=content.replace(ch, '')
                    r_log.append([invite_to_char, groupbydf.Key.iloc[i], r, content])
                else:
                    print('Error Accepting Invitation: Invitation ID Not found.')
                    r=None
                    r_log.append([invite_to_char, groupbydf.Key.iloc[i], 'Error Accepting Invitation: Invitation ID Not found.', 'Error Accepting Invitation: Invitation ID Not found.'])
    
                # r_log.append(r)
        else:
            print('should not be here')
            r_log.append(invite_to_char, None,'error','could not sign in as charity')

        r_log=pd.Series(r_log)

        return r_log

def get_current_schedules(fvuser, schedules_path=None, dlist=None, windows=None):

    ## todo split schedules/waitlists here instead of get_fn_schedules

    schedules, wls = get_fn_schedules(fvuser, donors_list=dlist, windows=windows)

    ## merge both with donors_df to get name and official id. Todo: rearrange cols for user logs
    schedules=pd.merge(fvuser.donors_df, schedules, how='inner', left_on='uuid', right_on='uuid')
    wls=pd.merge(fvuser.donors_df, wls.reset_index(), how='inner', left_on='uuid', right_on='uuid')

    ## set 'key' col to  official id, todo set to name if official id is null
    schedules.loc[:,'key']=schedules.official_id

    ## parse time cols, set donation window day 
    timecols=['Donation Starts At', 'Donation Ends At', 'Collection Starts At', 'Collection Ends At']
    for timecol in timecols:
        schedules[timecol]=schedules[timecol].apply(lambda x: strToTime(x, '%H:%M:%S'))

    # def get_posting_day(x):
    #     try:
    #         is_morning_collection=x['Collection Starts At']<x['Donation Starts At']
    #         print(is_morning_collection, ' ismorning collection')
    #         if is_morning_collection:
    #             posting_day=(x.DayOfWeek-1)%7
    #         else:
    #             posting_day=x.DayOfWeek
    #     except:
    #         posting_day=x.DayOfWeek

    #     return posting_day

    
    # # schedules['Posting Day'] = schedules.apply(lambda x: (x.DayOfWeek-1)%6 if x['Collection Starts At']<x['Donation Starts At'] else x.DayOfWeek, axis=1 ).astype(str)
    # schedules['Posting Day'] = schedules.apply(get_posting_day, axis=1).astype(str)
    # schedules['DayOfWeek'] = schedules.DayOfWeek.astype(str)

    # print(schedules.apply(print))

    ## separate upload files for primary and waitlist
    # primary=schedules.loc[pd.notna(schedules.primary), ['uuid', 'official_id', 'DayOfWeek','primary', 'primary_uuid', 'primary_name', 'primary_storage'
    #                                                         'invite_to', 'invite_to_uuid', 'invite_to_name']]
    primary_cols=['uuid', 'official_id', 'DayOfWeek','primary', 'primary_uuid', 'primary_name', 'storage', 'primary_start_date',
                                                            'invite_to', 'invite_to_uuid', 'invite_to_name', 'invite_start_date', 'Posting Day']
    
    primary=schedules.reindex(columns=primary_cols)
    # print(primary.head)
    # primary.columns=['uuid','official_id', 'DayOfWeek','primary', 'primary_uuid']
    primary.loc[:,'uninvite']='uninvite'
    primary.loc[:,'add_primary']='add'

    ## drop primary and waitlist cols from schedules
    schedules=schedules.drop(['primary','primary_uuid', 'primary_name', 'storage', 'primary_start_date',
                               'invite_to', 'invite_to_name', 'invite_to_uuid', 'invite_start_date' ,'waitlist'], axis=1)


    # schedules.to_csv(schedules_path+'scheds_test.csv')
    # primary.to_csv(schedules_path+'primary_test.csv')
    # wls.to_csv(schedules_path+'wls_test.csv')



    return schedules, primary, wls

# def get_current_schedules(fvuser, schedules_path=None, dlist=None):

#     ## todo split schedules/waitlists here instead of get_fn_schedules

#     schedules, wls = get_fn_schedules(fvuser, donors_list=dlist)

#     ## merge both with donors_df to get name and official id. Todo: rearrange cols for user logs
#     schedules=pd.merge(fvuser.donors_df, schedules, how='inner', left_on='uuid', right_on='uuid')
#     wls=pd.merge(fvuser.donors_df, wls.reset_index(), how='inner', left_on='uuid', right_on='uuid')

#     ## set 'key' col to  official id, todo set to name if official id is null
#     schedules.loc[:,'key']=schedules.official_id

#     ## parse time cols, set donation window day 
#     timecols=['Donation Starts At', 'Donation Ends At', 'Collection Starts At', 'Collection Ends At']
#     for timecol in timecols:
#         schedules[timecol]=schedules[timecol].apply(lambda x: strToTime(x, '%H:%M:%S'))

#     # def get_posting_day(x):
#     #     try:
#     #         is_morning_collection=x['Collection Starts At']<x['Donation Starts At']
#     #         print(is_morning_collection, ' ismorning collection')
#     #         if is_morning_collection:
#     #             posting_day=(x.DayOfWeek-1)%7
#     #         else:
#     #             posting_day=x.DayOfWeek
#     #     except:
#     #         posting_day=x.DayOfWeek

#     #     return posting_day

    
#     # # schedules['Posting Day'] = schedules.apply(lambda x: (x.DayOfWeek-1)%6 if x['Collection Starts At']<x['Donation Starts At'] else x.DayOfWeek, axis=1 ).astype(str)
#     # schedules['Posting Day'] = schedules.apply(get_posting_day, axis=1).astype(str)
#     # schedules['DayOfWeek'] = schedules.DayOfWeek.astype(str)

#     # print(schedules.apply(print))

#     ## separate upload files for primary and waitlist
#     # primary=schedules.loc[pd.notna(schedules.primary), ['uuid', 'official_id', 'DayOfWeek','primary', 'primary_uuid', 'primary_name', 'primary_storage'
#     #                                                         'invite_to', 'invite_to_uuid', 'invite_to_name']]
#     primary_cols=['uuid', 'official_id', 'DayOfWeek','primary', 'primary_uuid', 'primary_name', 'storage', 'primary_start_date',
#                                                             'invite_to', 'invite_to_uuid', 'invite_to_name', 'invite_start_date', 'Posting Day']
    
#     primary=schedules.reindex(columns=primary_cols)
#     # print(primary.head)
#     # primary.columns=['uuid','official_id', 'DayOfWeek','primary', 'primary_uuid']
#     primary.loc[:,'uninvite']='uninvite'
#     primary.loc[:,'add_primary']='add'

#     ## drop primary and waitlist cols from schedules
#     schedules=schedules.drop(['primary','primary_uuid', 'primary_name', 'storage', 'primary_start_date',
#                                'invite_to', 'invite_to_name', 'invite_to_uuid', 'invite_start_date' ,'waitlist'], axis=1)


#     # schedules.to_csv(schedules_path+'scheds_test.csv')
#     # primary.to_csv(schedules_path+'primary_test.csv')
#     # wls.to_csv(schedules_path+'wls_test.csv')



#     return schedules, primary, wls
# def get_current_schedules(fvuser, schedules_path, dlist=None):

#     ## todo split schedules/waitlists here instead of get_fn_schedules

#     schedules, wls = get_fn_schedules(fvuser, donors_list=dlist)

#     ## merge both with donors_df to get name and official id. Todo: rearrange cols for user logs
#     schedules=pd.merge(fvuser.donors_df, schedules, how='inner', left_on='uuid', right_on='uuid')
#     wls=pd.merge(fvuser.donors_df, wls.reset_index(), how='inner', left_on='uuid', right_on='uuid')

#     ## set 'key' col to  official id, todo set to name if official id is null
#     schedules.loc[:,'key']=schedules.official_id


#     ## separate upload files for primary and waitlist
#     # primary=schedules.loc[pd.notna(schedules.primary), ['uuid', 'official_id', 'DayOfWeek','primary', 'primary_uuid', 'primary_name', 'primary_storage'
#     #                                                         'invite_to', 'invite_to_uuid', 'invite_to_name']]
#     primary_cols=['uuid', 'official_id', 'DayOfWeek','primary', 'primary_uuid', 'primary_name', 'storage',
#                                                             'invite_to', 'invite_to_uuid', 'invite_to_name']
#     primary=schedules.reindex(columns=primary_cols)
#     # primary.columns=['uuid','official_id', 'DayOfWeek','primary', 'primary_uuid']
#     primary.loc[:,'uninvite']='uninvite'
#     primary.loc[:,'add_primary']='add'

#     ## drop primary and waitlist cols from schedules
#     schedules=schedules.drop(['primary','primary_uuid', 'primary_name', 'storage',
#                                 'invite_to', 'invite_to_uuid', 'invite_to_name', 'waitlist'], axis=1)


#     # schedules.to_csv(schedules_path+'scheds_test.csv')
#     # primary.to_csv(schedules_path+'primary_test.csv')
#     # wls.to_csv(schedules_path+'wls_test.csv')



#     return schedules, primary, wls

if __name__=='__main__':
    # token=fv.authenticate_fv('admin.foodbank@fareshare.org.uk', 'F00D1ver$e')
    ## fv instance
    dir_path = os.path.dirname(os.path.realpath(__file__))
    cache_path=os.path.join(dir_path,'Cache','prod')
    fvuser=FvUser('orgex@foodcloud.ie', fv.PW, cache_path=cache_path)
    # cw=fv.get_donor_collection_windows(token, 'd8fb1408-2312-4b3c-8b7a-78ca5f877e6f')
    # scheds, wls=get_fn_schedules(fvuser,  donors_list=['ALDI_IE:872007','ALDI_IE:873012','ALDI_IE:872019', 'ALDI_IE:872010'])
    scheds, prim, wls=get_current_schedules(fvuser,  dlist=['ALDI_IE:872007','ALDI_IE:873012','ALDI_IE:872019', 'ALDI_IE:872010'])

    print(wls)
    