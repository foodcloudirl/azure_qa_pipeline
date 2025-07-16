
from sqlalchemy.sql.sqltypes import ARRAY, JSON, TIMESTAMP, BigInteger, Boolean
from sqlalchemy.dialects.postgresql import JSONB

try:
    import foodiverse.fv_util as fv
except:
    import fv_util as fv

import datetime
import json

import requests
import pandas as pd
import sqlalchemy as sql
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData

import traceback
import sys


def get_network_member(token, membership_uuid):
    
    url=fv.api_url+f'/api/foodnet_admin/network_memberships/{membership_uuid}'
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
        return r, None

def get_db_members(token):
    '''retrieve membership data from fn_api endpoint in dict with db headers'''

    members = fv.get_fn_members(token)

    members_list=[db_members_fields(member) for member in members]

    return members_list

def db_members_fields(nm_data):
    '''get required db fields from memberships api response
        args:
        member dict api response
        return:
        member dict with keys corresponding to db headers'''
    member={}
    try:
        member['nm_uuid']=nm_data['uuid']
    except:
        member['nm_uuid']=nm_data['nm_uuid']
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
    member['parent_org']=nm_data['branch']['organisation']['name']
    try:
        member['storage_types']=nm_data['branch']['storage_types']
    except:
        member['storage_types']=None
    member['branch_isactive']=nm_data['branch']['is_active']
    addr_keys=['countries_id', 'provinces_id', 'counties_id', 'address_1', 'address_2', 'city_town', 'post_code', 'latitude', 'longitude', 'official_id']
    for key in addr_keys:
        member[key]=nm_data['branch'][key]
    member['prov']=member.pop('provinces_id')
    return member

def insert_members_row(member, conn, table):
    '''insert member dict with db headers to memberships table'''

    member['start_time']=datetime.datetime.now()
    member['end_time']=None
    # member['id']=3
    ins_query=table.insert().values([{**member}])
    print([{**member}])
    ins=conn.execute(ins_query)
    # try:
    #     ins=conn.execute(ins_query)
    # except sql.SqlAlchemyError:
    #     pass

    return ins




def update_member(member, conn, table):
    '''
    insert membership row, 
    if membership uuid exists, update membership(maxid) end time to now '''
    #set start time to now()
    # member['start_time']=datetime.datetime.now()
    # member['end_time']=None
    nm_uuid=member['nm_uuid']

    query= f"select * from memberships where nm_uuid='{member['nm_uuid']}' order by id desc limit (1) "
    # query=table.select().where(table.c.nm_uuid==member['nm_uuid']).order_by(sql.desc('start_time')).first()
    slct=conn.execute(query)
    # print('here')
    if slct.rowcount!=0:
        for row in slct:
            id=row['id']
        print(f'{id} row for existing entry for membership {nm_uuid}')
        upd_query=table.update().where(table.c.id==id).values(end_time=datetime.datetime.now())
        upd=conn.execute(upd_query)
        
    else:
        # logging.info('New membership')
        print('new membership')
        pass

    ins=insert_members_row(member, conn, table)

    return ins


def insert_donation_window(window,conn, table):
    
    window['start_time']=datetime.datetime.now()
    window['end_time']=None

    # member['id']=3
    ins_query=table.insert().values([{**window}])
    print([{**window}])
    # ins=conn.execute(ins_query)
    try:
        ins=conn.execute(ins_query)
        return ins
    except Exception as e:
        stack = traceback.extract_stack()
        (filename, line, procname, text) = stack[-1]
        print(stack[-1])
        print(line)
        print(procname) # function name
        print(filename) # module name
        print('Unhandled error occoured: ', sys.exc_info())
        return e

    

def create_membership_table(meta, engine):

    
    memberships=Table('memberships', meta,
    Column('id', Integer, primary_key=True),
    Column('nm_uuid', String),
    Column('type', String),
    Column('network_uuid', String),
    Column('network_org_uuid', String),
    Column('network_org_name', String),
    Column('current_status', String),
    Column('created_at', TIMESTAMP),
    Column('updated_at', TIMESTAMP),
    Column('branch_uuid', String),
    Column('branch_name', String),
    Column('parent_org', String),
    Column('storage_types', JSONB),
    Column('branch_isactive', String),
    Column('countries_id', String),
    Column('prov', String),
    Column('counties_id', String),
    Column('address_1', String),
    Column('address_2', String),
    Column('city_town', String),
    Column('post_code', String),
    Column('latitude', String),
    Column('longitude', String),
    Column('official_id', String),
    Column('is_deleted', Boolean),
    Column('start_time', TIMESTAMP),
    Column('end_time', TIMESTAMP))

    meta.create_all(engine)

def create_donation_windows_table(meta, engine):

    with engine.connect() as conn:
        q='DROP TABLE IF EXISTS donation_windows;'
        qr=conn.execute(q)

    donation_windows = Table('donation_windows', meta,
    Column('id', BigInteger, primary_key=True, autoincrement=True),
    Column('uuid', String),
    Column('nm_uuid', String),
    Column('branch_uuid', String),
    Column('starts_at', String),
    Column('ends_at', String),
    Column('day_of_week', String),
    Column('is_active', String),
    Column('created_at', TIMESTAMP),
    Column('updated_at', TIMESTAMP),
    Column('start_time', TIMESTAMP),
    Column('end_time', TIMESTAMP),
    Column('is_deleted', Boolean)
    )
    meta.create_all(engine)
    print('success')

    return None

def create_collection_windows_table(meta, engine):

    with engine.connect() as conn:
        q='DROP TABLE IF EXISTS collection_windows;'
        qr=conn.execute(q)

    donation_windows = Table('collection_windows', meta,
    Column('id', BigInteger, primary_key=True, autoincrement=True),
    Column('uuid', String),
    Column('nm_uuid', String),
    Column('branch_uuid', String),
    Column('transfer_types_id', String),
    Column('starts_at', String),
    Column('ends_at', String),
    Column('day_of_week_start', String),
    Column('day_of_week_end', String),
    Column('max_collectors', String),
    Column('difference_in_days', String),
    Column('primary_uuid', String),
    Column('primary_expires_at', TIMESTAMP),
    Column('primary_x_days_from_acceptance', String),
    Column('primary_starts_at_date', TIMESTAMP),
    Column('primary_created_at', TIMESTAMP),
    Column('primary_updated_at', TIMESTAMP),
    Column('primary_tsm_current_state', String),
    Column('primary_scheduled_activation_date', TIMESTAMP),
    Column('primary_tsm_current_state_label', String),
    Column('primary_branch_uuid', String),
    Column('primary_branch_name', String),
    Column('primary_branch_official_id', String),
    Column('invited_uuid', String),
    Column('invited_expires_at', TIMESTAMP),
    Column('invited_x_days_from_acceptance', String),
    Column('invited_starts_at_date', TIMESTAMP),
    Column('invited_created_at', TIMESTAMP),
    Column('invited_updated_at', TIMESTAMP),
    Column('invited_tsm_current_state', String),
    Column('invited_scheduled_activation_date', TIMESTAMP),
    Column('invited_tsm_current_state_label', String),
    Column('invited_branch_uuid', String),
    Column('invited_branch_name', String),
    Column('invited_branch_official_id', String),
    Column('created_at', TIMESTAMP),
    Column('updated_at', TIMESTAMP),
    Column('start_time', TIMESTAMP),
    Column('end_time', TIMESTAMP),
    Column('waitlists', JSONB),
    Column('is_deleted', Boolean)
    )
    meta.create_all(engine)
    print('success')

    return None



def load_members(table, conn, member_list):
    ''' For first data load- use memberships bulk api endpoint to get list of all members and insert to db'''
    for member in member_list:
        ins=update_member(member, conn, table)


def db_dw_fields(dw, nm_uuid, branch_uuid):
    ''' Args:
            dw: a donation window from windows response, nm_uuid of parent donor org
        (which is output of dw.__init__.window_from_response - filters window id from parent org windows response (get_donor_windows)
        returns:  dict with format required for db insert'''
    
    dw['nm_uuid']=nm_uuid
    dw['branch_uuid']=branch_uuid

    return dw

def db_cw_fields(cw, nm_uuid, branch_uuid):
    ''' Args:
            cw: a collection window from windows response, nm_uuid of parent donor org
        (which is output of cw.__init__.window_from_response - filters window id from parent org windows response (get_donor_windows)
        returns:  dict with format required for db insert'''

    cwdb={}
    if len(cw)==0:
        return cwdb
    
    cwdb['nm_uuid']=nm_uuid
    cwdb['branch_uuid']=branch_uuid
    asis_keys=['uuid', 'transfer_types_id', 'starts_at', 'ends_at', 'day_of_week_start', 'day_of_week_end', 'created_at', 'updated_at', 'max_collectors', 'difference_in_days']
    for key in asis_keys:
        cwdb[key]=cw[key]
    # primary_map=[{'cw':'uuid', 'db':'primary_uuid'}, {'cw':'expires_at' }]
    if cw['primary_charity_branch'] is not None:
        cwdb['primary_uuid']=cw['primary_charity_branch']['uuid']
        cwdb['primary_expires_at']=cw['primary_charity_branch']['expires_at']
        cwdb['primary_x_days_from_acceptance']=cw['primary_charity_branch']['starts_at_x_days_from_acceptance']
        cwdb['primary_starts_at_date']=cw['primary_charity_branch']['starts_at_date']
        cwdb['primary_created_at']=cw['primary_charity_branch']['created_at']
        cwdb['primary_updated_at']=cw['primary_charity_branch']['updated_at']
        cwdb['primary_tsm_current_state']=cw['primary_charity_branch']['tsm_current_state']
        cwdb['primary_scheduled_activation_date']=cw['primary_charity_branch']['scheduled_activation_date']
        cwdb['primary_tsm_current_state_label']=cw['primary_charity_branch']['tsm_current_state_label']
        cwdb['primary_branch_uuid']=cw['primary_charity_branch']['charity_branch']['uuid']
        cwdb['primary_branch_name']=cw['primary_charity_branch']['charity_branch']['name']
        cwdb['primary_branch_official_id']=cw['primary_charity_branch']['charity_branch']['official_id']

       
    if cw['invited_branch'] is not None:
        cwdb['invited_uuid']=cw['invited_branch']['uuid']
        cwdb['invited_expires_at']=cw['invited_branch']['expires_at']
        cwdb['invited_x_days_from_acceptance']=cw['invited_branch']['starts_at_x_days_from_acceptance']
        cwdb['invited_starts_at_date']=cw['invited_branch']['starts_at_date']
        cwdb['invited_created_at']=cw['invited_branch']['created_at']
        cwdb['invited_updated_at']=cw['invited_branch']['updated_at']
        cwdb['invited_tsm_current_state']=cw['invited_branch']['tsm_current_state']
        cwdb['invited_scheduled_activation_date']=cw['invited_branch']['scheduled_activation_date']
        cwdb['invited_tsm_current_state_label']=cw['invited_branch']['tsm_current_state_label']
        cwdb['invited_branch_uuid']=cw['invited_branch']['charity_branch']['uuid']
        cwdb['invited_branch_name']=cw['invited_branch']['charity_branch']['name']
        cwdb['invited_branch_official_id']=cw['invited_branch']['charity_branch']['official_id']

    if len(cw['collection_window_waitlist'])>0:
        wls=[]
        wl_response=cw['collection_window_waitlist']
        for wl in wl_response:
            e={}
            e['branch_uuid']=wl['charity_branch']['uuid']
            e['branch_name']=wl['charity_branch']['name']
            e['branch_official_id']=wl['charity_branch']['official_id']

            wls.append(e)
        cwdb['waitlists']=wls

    


    return cwdb
    

def update_dw(dw, conn, table, logging=None):
    '''
    for updated type : insert window row, 
    if window uuid exists, update membership(maxid) end time to now '''
    #set start time to now()
    # member['start_time']=datetime.datetime.now()
    # member['end_time']=None
    uuid=dw['uuid']

    # query= f"select * from donation_windows where uuid='{uuid}' order by id desc limit (1) "
    # query= f"select * from donation_windows where uuid='{uuid}' order by id desc "
    query=table.select().where(table.c.uuid==uuid).order_by(sql.desc('id')).limit(1)
    slct=conn.execute(query)

    if slct.rowcount!=0:
        for row in slct:
            id=row['id']
            if logging:
                logging.info(f'{id} row for existing entry for window {uuid}')
            print(f'{id} row for existing entry for window {uuid}')
            upd_query=table.update().where(table.c.id==id).values(end_time=datetime.datetime.now())
            try:
                upd=conn.execute(upd_query)
                if logging:
                    logging.info('updated row')
            except Exception as e:
                err='Error in update existing row: ' + sys.exc_info()
        
    else:
        # logging.info('New window')
        print('new window')
        pass
    try:
        ins=insert_donation_window(dw, conn, table)
        err=None
    except Exception as e:
        err='Error in insert row: ' + sys.exc_info()

    return err

def delete_window(window_uuid, conn, table):
    # upd_is_del=table.update().where(table.c.uuid==window_uuid).values({'is_deleted':True})
    # res=conn.execute(upd_is_del)
    
    # upd_end_time=table.update().where(table.c.uuid==window_uuid).order_by(sql.desc('id')).limit(1).values({'end_time':datetime.datetime.now()})
    # res=conn.execute(upd_end_time)

    query=table.select().where(table.c.uuid==window_uuid).order_by(sql.desc('id')).limit(1)
    slct=conn.execute(query)

    if slct.rowcount!=0:
        for row in slct:
            id=row['id']
            upd_query=table.update().where(table.c.id==id).values({'end_time':datetime.datetime.now(), 'is_deleted':True})
            try:
                res=conn.execute(upd_query)
                # if logging:
                #     logging.info('updated row')
            except Exception as e:
                err='Error in update existing row: ' + sys.exc_info()
                res=None


    return res

def get_donor_dw_list(donor_uuid, token):

    dw_list=fv.get_donor_windows(token, donor_uuid)

def window_from_response(windowid, response):
    for dw_day in response:
        for dw in dw_day:
            if dw['uuid']==windowid:
                updated_window=dw
                return updated_window
    return None

def validate_window_payload(params, valid_types, logging, func):

    if 'MessageType' in params.keys():
        message_type=params['MessageType']
        logging.info(f'messagr type fpund: {message_type}')

        if message_type not in valid_types:
            logging.error(f"invalid message type {message_type}")
            return func.HttpResponse(f'invalid message type {message_type}', status_code=400), f"invalid message type {message_type}"
    
    else:
        logging.error('Message Type not found in keys')
        return func.HttpResponse('Message Type not found in keys', status_code=400), 'Message Type not found in keys'
    
    
    req_keys=['MembershipId', 'BranchId' , 'EntityId']
    for key in req_keys:
        if key not in params.keys():
            logging.error(f'A required key {key} not found in keys {params.keys()}')
            return func.HttpResponse(f'A required key {key} not found in keys {params.keys()}', status_code=400), f'A required key {key} not found in keys {params.keys()}'
    # logging.info(f'keys found in {params.keys()}')

    return None, None

def donor_windows_to_dict(dw_df=None, cw_df=None):

    '''jsonify-able dict windows to return in httpsResponse'''

    uuids=[]
    if dw_df is not None:
        uuids = uuids + dw_df.nm_uuid.to_list()
    if cw_df is not None:
        uuids = uuids + cw_df.nm_uuid.to_list()

    uuids = set(uuids)
    print(uuids)

    windows={}

    for uuid in uuids:
        print(uuid)
        dw_list = [[],[],[],[],[],[],[]]
        cw_list = [[],[],[],[],[],[],[]]
        windows[uuid]={}
        

        for dow in range(1,8):
            if dw_df is not None:
                dow_dw_df=dw_df.loc[ (dw_df['nm_uuid']==uuid) & (dw_df['day_of_week']==str(dow)),:]
                if len(dow_dw_df)>0:
                    ## call transform on x.to_dict() before calling to_list
                    dow_list = dow_dw_df.apply(lambda x: x.to_dict(), axis=1)
                    print(dow_list)
                    dow_list = dow_list.apply(lambda x: transform_dw(x)).to_list()
                    dw_list[dow-1]=dow_list

            if cw_df is not None:
                dow_cw_df=cw_df.loc[ (cw_df['nm_uuid']==uuid) & (cw_df['day_of_week_start']==str(dow)),:]
                if len(dow_cw_df)>0:
                    dow_list = dow_cw_df.apply(lambda x: x.to_dict(), axis=1)
                    print(dow_list)
                    dow_list = dow_list.apply(lambda x: transform_cw(x)).to_list()
                    cw_list[dow-1]=dow_list

        windows[uuid]['donation_windows']=dw_list
        windows[uuid]['collection_windows']=cw_list

    return windows


def transform_dw(dw):
    '''
    transform dw to structure from API to reuse
    '''
    return dw

def transform_cw(cw):
    '''
    transform dw to structure from API to reuse
    '''
    cw_api={}
    asis_keys=['uuid', 'transfer_types_id', 'starts_at', 'ends_at', 'day_of_week_start', 'day_of_week_end', 'created_at', 'updated_at', 'max_collectors', 'difference_in_days']
    for key in asis_keys:
        if key in cw.keys():
            # if key in ['starts_at', 'ends_at', 'created_at', 'updated_at']:
            #     cw_api[key]=cw[key].astype(str)
            cw_api[key]=cw[key]

    # dt_keys = ['start_time', 'end_time', 'primary_starts_at_date', 'primary_scheduled_activation_date', 'invited_starts_at_date', 'invited_scheduled_activation_date']
    # for key in dt_keys:
    #     if key in cw.keys():
    #         if isinstance(cw[key], (datetime.datetime, datetime.date)):
    #             cw[key] = cw[key].strftime('%Y-%m-%d')
    
    if cw['primary_branch_uuid']:
        cw_api['primary_charity_branch']={}
        cw_api['primary_charity_branch']['charity_branch']={}
        cw_api['primary_charity_branch']['uuid']=cw['primary_uuid']
        # cw_api['primary_charity_branch']['expires_at']=cw['primary_expires_at']
        # cw_api['primary_charity_branch']['starts_at_x_days_from_acceptance']=cw['primary_x_days_from_acceptance']
        cw_api['primary_charity_branch']['starts_at_date']=cw['primary_starts_at_date']
        # cw_api['primary_charity_branch']['created_at']=cw['primary_created_at']
        # cw_api['primary_charity_branch']['updated_at']=cw['primary_updated_at']
        cw_api['primary_charity_branch']['tsm_current_state']=cw['primary_tsm_current_state']
        cw_api['primary_charity_branch']['scheduled_activation_date']=cw['primary_scheduled_activation_date']
        # cw_api['primary_charity_branch']['tsm_current_state_label']=cw['primary_tsm_current_state_label']
        cw_api['primary_charity_branch']['charity_branch']['uuid']=cw['primary_branch_uuid']
        cw_api['primary_charity_branch']['charity_branch']['name']=cw['primary_branch_name']
        cw_api['primary_charity_branch']['charity_branch']['official_id']=cw['primary_branch_official_id']
    else: cw_api['primary_charity_branch']=None

    if cw['invited_branch_uuid']:
        cw_api['invited_branch']={}
        cw_api['invited_branch']['charity_branch']={}
        cw_api['invited_branch']['uuid']=cw['invited_uuid']
        cw_api['invited_branch']['starts_at_date']=cw['invited_starts_at_date']
        cw_api['invited_branch']['tsm_current_state']=cw['invited_tsm_current_state']
        cw_api['invited_branch']['scheduled_activation_date']=cw['invited_scheduled_activation_date']
        cw_api['invited_branch']['charity_branch']['uuid']=cw['invited_branch_uuid']
        cw_api['invited_branch']['charity_branch']['name']=cw['invited_branch_name']
        cw_api['invited_branch']['charity_branch']['official_id']=cw['invited_branch_official_id']
    else: cw_api['invited_branch']=None


    if cw['waitlists'] is not None:
        cw_api['collection_window_waitlist']=cw['waitlists']
    else:
        cw_api['collection_window_waitlist']={}


    


    return cw_api



    

def get_dws(donor_uuid_list, conn):
    donor_uuid_list = ["'"+x+"'" for x in donor_uuid_list]
    donors_str = ','.join(donor_uuid_list)

    q=f"select dw.id, dw.uuid, dw.nm_uuid , m.branch_name, dw.starts_at , dw.ends_at , dw.day_of_week , dw.is_active, dw.start_time , dw.end_time \
        from donation_windows dw \
        join (\
            select uuid , max(id) as id from donation_windows dw2 group by uuid ) t \
        on dw.id = t.id \
        join memberships m on dw.nm_uuid = m.nm_uuid \
        where dw.nm_uuid in  ({donors_str});"

    df = pd.read_sql_query(q, conn)

    return df

def get_cws(donor_uuid_list, conn):
    donor_uuid_list = ["'"+x+"'" for x in donor_uuid_list]
    donors_str = ','.join(donor_uuid_list)

    q=f"select cw.id, cw.uuid, cw.nm_uuid , m.branch_name, cw.starts_at , cw.ends_at , cw.day_of_week_start , cw.max_collectors, \
        cw.primary_branch_uuid , cw.primary_branch_name, cw.primary_branch_official_id , cw.primary_starts_at_date , Date(cw.primary_scheduled_activation_date) as primary_scheduled_activation_date , cw.primary_tsm_current_state , cw.primary_uuid, \
        cw.invited_uuid, cw.invited_branch_uuid , cw.invited_branch_name, cw.invited_branch_official_id, cw.invited_starts_at_date, cw.invited_tsm_current_state , Date(cw.invited_scheduled_activation_date) as invited_scheduled_activation_date ,\
        cw.waitlists ,\
        cw.start_time , cw.end_time \
        from collection_windows cw \
        join ( \
            select uuid , max(id) as id from collection_windows cw2 group by uuid ) t \
        on cw.id = t.id \
        join memberships m on cw.nm_uuid = m.nm_uuid \
        where cw.nm_uuid in ({donors_str});"

    df = pd.read_sql_query(q, conn)

    return df


def get_uploader_windows(uuids_list, conn, donation_windows=True, collection_windows=True):

    if donation_windows:
        dw_df = get_dws(uuids_list, conn)
    else:
        dw_df=None

    if collection_windows:
        cw_df = get_cws(uuids_list, conn)

    windows = donor_windows_to_dict(dw_df, cw_df)

    return windows

if __name__ == '__main__':

    engine=create_engine('postgresql+psycopg2://dbadmin:DbPg2107!@fv-rep-v1.postgres.database.azure.com:5432/postgres?sslmode=require', echo=False) 
    meta=MetaData()
    conn=engine.connect()

    token = fv.authenticate_fv('admin.foodbank@fareshare.org.uk', 'F00D1ver$e')

    # r, member_response=get_network_member(token,'9ec51f87-877d-436a-bf43-d50d4946176a')
    uuids_list=['2367a22f-40a8-4b3c-94eb-3012aa3c51a1']
    # dws = get_dws(['9ec51f87-877d-436a-bf43-d50d4946176a'], conn)
    # cws = get_cws(['9ec51f87-877d-436a-bf43-d50d4946176a'], conn)

    windows = get_uploader_windows(uuids_list, conn, donation_windows=True, collection_windows=True)


    
        


