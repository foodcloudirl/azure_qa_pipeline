import fv_util as fv

import pandas as pd

import datetime

if __name__ == '__main__':

    emails=["retailer@aldi.ie", "retailer@lidl.ie", "apps+musgraveie@foodcloud.ie", "retailer@dunnesstore.ie"]
    pws = [ "$iC2.azWDVP954", "qLfps?5d30dU$sE", "yPlde245”?&g4DX3" , "gDVC?%5CVuiq5”" ]
    donors = ['aldi', 'lidl', 'musgrave', 'dunnes']
    donations_dict={}

    for i,email in enumerate(emails):

        pw = pws[i]
        donor = donors[i]
        donor_token = fv.authenticate_fv(email, pw)

        donations_list, response_list = fv.get_donations(donor_token, 1, 30)

        donations_dict[donor]={}
        donations_dict[donor]['donations']=donations_list
        donations_dict[donor]['responses']=response_list

    keep_fields_donations = ['external_id', 'donation_date', 'donation_time', 'donation_uuid', 'branch_uuid', 'branch_name', 'official_id', 'ambient_total', 'chilled_total', 'frozen_total', 'hot_total', 'total', 'time_left', 'created_at', 'tsm_current_state', 'donation_window_start_at', 'donation_window_end_at', 'foodboard_available_at', 'primary_available_at', 'accepted_by', 'tsm_current_state_label']
    keep_fields_responses = ['uuid', 'type', 'token', 'response_at', 'created_at', 'updated_at', 'deleted_at', 'tsm_current_state', 'uncollected_reason', 'accepted_at', 'declined_at', 'transferred_at', 'not_transferred_at', 'tsm_current_state_label', 'items', "charity_branch['name']"]
    # process to dfs
    summary_dict={}
    wd=[]
    wd_r=[]
    for donor in donors:
        summary_dict[donor]={}
        donations = donations_dict[donor]['donations']
        summary_dict[donor]['total_donations']=len(donations)
        weekend_donations = 0
        wl_response=0
        for donation in donations:
            d = datetime.datetime.strptime(donation['donation_date'], '%Y-%m-%d')
            dow = d.weekday()
            if dow in range(4,7):
                weekend_donations+=1
                donation_filt = {key: donation[key] for key in keep_fields_donations}
                donation_filt['response_type']=None
                donation_filt['wl_char_response']=None
                if len(donation['responses'])==0:
                    donation_filt['response_type']='No response'
                    
                # elif 'W' in wd['responses'].keys():
                elif 'W' in donation['responses'].keys():
                    donation_filt['response_type']='W'
                    donation_filt['wl_char_response'] = donation['responses']['W'][0]['charity_branch']['name']
                    wl_response+=1
                else:
                    donation_filt['response_type']='Non W'

                wd.append(donation_filt)

            summary_dict[donor]['weekend_donations']=weekend_donations
            summary_dict[donor]['wl_response_count']=wl_response
                
            


    '''
    >>> for donation in donations_dict['lidl']['donations'][250:350]:
...     print(donation['donation_date'])
...     date_parsed = datetime.datetime.strptime(donation['donation_date'], '%Y-%M-%d')
...     dow = date_parsed.weekday()
...     if dow in range(4,7):
...             print(f'dow found {dow}')
...     else:
...             print(f'not a weekend {dow}')
'''