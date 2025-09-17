import os
import shutil
import chardet
import sys
import logging
import json
import math
import csv
import requests
from pytz import timezone
from datetime import datetime,timedelta
import tzlocal
import argparse
import random
from zoom_client.controller import controller
import pandas as pd
import pkg_resources
from requests.utils import requote_uri
from urllib.parse import urlencode, quote_plus
import arrow
import time
import base64
from userfunctions import single_urlencode, double_urlencode, encode_uuid, convert_to_pst, uniq_algorithm
import urllib3
from urllib.parse import quote

if __name__ == "__main__":
   start = time.time()
   installed_packages = {d.project_name: d.version for d in pkg_resources.working_set}

   run_path = os.path.dirname(os.path.realpath(__file__))

   #reduce the number of informational messages from the requests lib
   logging.getLogger('requests').setLevel(logging.WARNING)

   #open config file with api key/secret information
   config_file = open("config.json")
   config_data = json.load(config_file)

   #create Zoom python client
   zoom = controller(config_data)

   start_date = arrow.now().shift(days=-31).format('YYYY-MM-DD')
   end_date = arrow.now().format('YYYY-MM-DD')
   print(start_date, end_date)
      
   latest_meeting_instances = zoom.dashboard.get_past_meetings(start_date, end_date)
   df_meeting_instances = pd.DataFrame(latest_meeting_instances)
   date_time = os.getenv('date_time')
   df_meeting_instances.to_csv('./zoom_etl/'+date_time+'/fact_uuid_last_28_days_'+date_time+'.csv', index= False)

   # Google Sheet API Key
   apiKey = "AIzaSyAFWIA_60-D6hAw3hlSOJT7W0gWnVTzYGM"  # Please set your API key.
   # Retrieve GID from Spreadsheet.
   spreadsheetId = "1xEDsfIFRD6LCjO0hHe543DAyqCyNmlOVo_Q46CLhFVE"  # This is the Spreadsheet ID of your Spreadsheet.
   url = "https://sheets.googleapis.com/v4/spreadsheets/" + spreadsheetId + "?fields=sheets.properties.sheetId&key=" + apiKey
   res = requests.get(url).json()
   print('Done with resquests get for the GoogleSheet')
   gid = "edit#gid=" + str(res["sheets"][0]["properties"]["sheetId"])
   baseURL = "https://docs.google.com/spreadsheets/d/1xEDsfIFRD6LCjO0hHe543DAyqCyNmlOVo_Q46CLhFVE/"
   fullURL = baseURL + gid
   sheet_url_one = fullURL
   csv_export_url_one = sheet_url_one.replace('/edit#gid=', '/export?format=csv&gid=')
   df_meetingids_one = pd.read_csv(csv_export_url_one)
   df_meetingids_one.to_csv('./Production_Service_Grouping/RS'+'/recurring_services_'+date_time+'.csv', index= False)
   df_meetingids_one = df_meetingids_one.loc[:,'Meeting ID']
   gid = "edit#gid=" + str(res["sheets"][1]["properties"]["sheetId"])
   baseURL = "https://docs.google.com/spreadsheets/d/1xEDsfIFRD6LCjO0hHe543DAyqCyNmlOVo_Q46CLhFVE/"
   fullURL = baseURL + gid
   sheet_url_two = fullURL
   csv_export_url_two = sheet_url_two.replace('/edit#gid=', '/export?format=csv&gid=')
   df_meetingids_two = pd.read_csv(csv_export_url_two)
   df_meetingids_two.to_csv('./Production_Service_Grouping/SP'+'/special_programs_'+date_time+'.csv', index= False)
   df_meetingids_two = df_meetingids_two.loc[:,'Meeting ID']
   gid = "edit#gid=" + str(res["sheets"][2]["properties"]["sheetId"])
   baseURL = "https://docs.google.com/spreadsheets/d/1xEDsfIFRD6LCjO0hHe543DAyqCyNmlOVo_Q46CLhFVE/"
   fullURL = baseURL + gid
   sheet_url_three = fullURL
   csv_export_url_three = sheet_url_three.replace('/edit#gid=', '/export?format=csv&gid=')
   df_meetingids_three = pd.read_csv(csv_export_url_three)
   df_meetingids_three.to_csv('./Production_Service_Grouping/PS'+'/program_segments_'+date_time+'.csv', index= False)
   df_meetingids = pd.concat([df_meetingids_one, df_meetingids_two]) 
   df_meetingids = list(set(df_meetingids))
   print('df_meeting_ids list from Google Sheet:', df_meetingids)
   df_meeting_uuids = df_meeting_instances[df_meeting_instances['id'].isin(df_meetingids)]
   fact_uuid_new = df_meeting_uuids.reset_index(drop=True)
   fact_uuid_new.to_csv('./zoom_etl/'+date_time+'/fact_uuid_new_filter_meeting_ids_'+date_time+'.csv', index= False)

   fact_uuid_old = pd.read_csv('fact_uuid_master.csv', index_col=False)
   # merge new and old fact uuid files and filter on _merge = Left_only to keep only the new uuids (latest extract's uuids)
   new_old_fact_uuid = fact_uuid_new.merge(fact_uuid_old, on='uuid', how='outer', suffixes=['', '_'], indicator=True)
   # new_old_fact_uuid.to_csv('new_old_fact_uuid.csv', index=False)
   new_only_fact_uuid = new_old_fact_uuid[new_old_fact_uuid['_merge'] == "left_only"]
   new_only_fact_uuid = new_only_fact_uuid.rename(columns={'host':'user_name', 'email':'user_email', 'participants':'participants_count'})
   new_only_fact_uuid = new_only_fact_uuid[['uuid', 'id', 'topic', 'user_name', 'user_email', 
                                            'start_time', 'end_time', 'duration', 'participants_count']]
   new_only_fact_uuid = new_only_fact_uuid.iloc[:,[0,1,2,3,5,7,8,9,10]]
   df_uuid_new_only = new_only_fact_uuid.reset_index(drop=True)
   df_uuid = new_only_fact_uuid.reset_index(drop=True)
   df_uuid = df_uuid.reset_index(drop=True)
   # df_uuid = fact_uuid_old - Set this for full download
   print('Size of UUID altered: ', df_uuid.shape)
   print('Size of current master_uuid : ', fact_uuid_old.shape)
   df_master_data = pd.DataFrame()
   count = 0
   print('Index of UUID dataframe: ',df_uuid.index)
   for i in range(0, len(df_uuid)):
      uuid = df_uuid.loc[i, 'uuid']
      uuid = encode_uuid(uuid)
      try:
         zoom_data = zoom.dashboard.get_past_meeting_participants(uuid)
         df_participants = pd.DataFrame(zoom_data)
         df_participants['meeting_id'] = df_uuid.loc[i, 'id']
         df_participants['uuid'] = df_uuid.loc[i, 'uuid']
         df_master_data = pd.concat([df_master_data,df_participants])
         print('df_master_data shape:', df_master_data.shape)
         df_uuid.loc[i, 'etl_status'] = 'Y'
         count = count + 1
         print('Count : ', count, ' Meeting ID : ', df_uuid.loc[i, 'id'], ' Meeting UUID : ', df_uuid.loc[i, 'uuid'], ' DONE')
      except:
         count = count + 1
         print('Count : ', count, ' Meeting ID : ', df_uuid.loc[i, 'id'], ' Meeting UUID : ', df_uuid.loc[i, 'uuid'], ' NOT DONE')
         continue

   df_uuid = df_uuid.reset_index(drop=True)
   df_uuid.to_csv('./zoom_etl/'+date_time+'/fact_uuid_etl_status_'+date_time+'.csv', index= False)
   print('df_master_data shape:', df_master_data.shape)
   ##########################################################################################
   # Select required columns and re-order in the Connection Data file
   ##########################################################################################
   columns_add = ['error_code','error', 'in_room_participants']
   for newcol in columns_add:
      if newcol not in df_master_data.columns:
         new_cols = [newcol]
         df_master_data = df_master_data.reindex(columns=df_master_data.columns.tolist() + new_cols)   # add empty cols
      else:
         pass
      
   df_master_data = df_master_data[['id', 'user_id', 'user_name', 'ip_address', 'location', 'network_type', 'data_center',
                                    'connection_type', 'join_time', 'leave_time', 'share_application', 'share_desktop',
                                    'share_whiteboard', 'recording', 'pc_name', 'domain', 'harddisk_id', 'version', 'leave_reason',
                                    'email', 'status', 'microphone', 'speaker', 'camera', 'meeting_id', 'uuid', 'error_code',
                                    'error', 'in_room_participants']]

   ##########################################################################################
   print('After in_room_participants df_master_data shape:', df_master_data.shape)
   df_master_data['updated_at'] = date_time
   df_master_data.to_csv('./zoom_etl/'+date_time+'/fact_connections_raw_'+date_time+'.csv', index= False)
   print('Fact_Connections Successful')
   # fact_uuid_old['updated_at'] = fact_uuid_old['end_time'].max()
   df_uuid_new_only['updated_at'] = date_time
   fact_uuid = pd.concat([fact_uuid_old, df_uuid_new_only])
   fact_uuid.to_csv('./zoom_etl/'+date_time+'/fact_uuid_master_'+date_time+'.csv', index= False)
   fact_uuid.to_csv('./Production_Fact_UUID/fact_uuid_master_'+date_time+'.csv', index= False)
   ## If file exists, delete it ##
   myfile="fact_uuid_master.csv"
   if os.path.isfile(myfile):
      os.remove(myfile)
      fact_uuid.to_csv('fact_uuid_master.csv', index= False)
   else:    ## Show an error ##
      print("Error: %s file not found" % myfile)
   fact_uuid_old.to_csv('./zoom_etl/'+date_time+'/fact_uuid_old_'+date_time+'.csv', index= False)
   df_uuid_new_only.to_csv('./zoom_etl/'+date_time+'/fact_uuid_new_'+date_time+'.csv', index= False)
   print('Fact_UUIDs Successful')
   end = time.time()
   hours, rem = divmod(end-start, 3600)
   minutes, seconds = divmod(rem, 60)
   print("{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))

   # Converts UTC join_time and leave_time to PT timezone. Also removes missing join_time, leave_time, ip_address, meeting_id rows.
   incremental = df_master_data.copy()
   incremental = incremental.dropna(how='all')
   incremental = incremental.dropna(axis=0, subset=['join_time', 'leave_time', 'ip_address', 'meeting_id'])
   # incremental['join_time'] = incremental.apply(lambda x: convert_to_pst(x['join_time']), axis = 1)
   # incremental['leave_time'] = incremental.apply(lambda x: convert_to_pst(x['leave_time']), axis = 1)
   # Create incremental index and sort by uuid and DID_Orig. Sorting by these variables is required to run the Uniques Algorithm successfully.

   ########################################################################################################
   ## Incremental Algorithm
   ########################################################################################################
   print("running uniques algorithm for incremental data..")
   incremental['user_name'] = incremental['user_name'].str.encode('ascii', 'ignore').str.decode('ascii')
   incremental['location'] = incremental['location'].str.encode('ascii', 'ignore').str.decode('ascii')
   startTime = datetime.now()
   inc = incremental
   common_list= ["Zoom user", "zoom user", "user_name",  "iPhone", "iPad", "Jai Guru",  "Yogananda", "John", "Jane", "Mary",  "Maria", "Paul",  "Redmi", "", " ", "  ", "   "] 
   inc['location']=inc.location.apply(str)
   inc['country']=inc['location'].apply(lambda st: st[st.rfind("(")+1:st.rfind(")")])
   inc.dropna(subset=['country'], inplace=True)
   inc.reset_index(drop=True, inplace=True)
   for i in range(0, len(inc)-1):
      print(i)
      print(inc.iloc[i, 2])
      if(inc.loc[i, 'user_name'] in common_list):
         inc.loc[i, 'user_name'] = str(inc.loc[i, 'user_name']) + "_" + str(inc.loc[i, 'location']) + "_" + str(inc.loc[i, 'country']) 
      print("isnull")
      if(pd.isnull(inc.loc[i, 'user_name'])):
         inc.loc[i, 'username_country'] = str(inc.loc[i, 'user_name']) + "_" + str(inc.loc[i, 'country'])
      else:
         inc.loc[i, 'username_country'] = str(inc.loc[i, 'user_name']) + "_" + str(inc.loc[i, 'country'])
    
   inc["DID_orig"] = range(1, 1+ len(inc))
   inc['DID'] = inc['DID_orig']
   inc.sort_values(by=['uuid', 'DID_orig'], ascending = False,  inplace = True)
   inc.reset_index(inplace =True)
    
   for i in range(0, len(inc)-1):
      print(i)
      j=1
      while(inc.loc[i, 'uuid'] == inc.loc[i+j, 'uuid']):
         if((inc.loc[i, 'username_country'] == inc.loc[i+j, 'username_country'] and incremental.loc[i, 'username_country'] != "") or (inc.loc[i, 'email'] == inc.loc[i+j, 'email'] and inc.loc[i, 'email'] != "")):
            inc.loc[i, 'DID'] = inc.loc[i+j, 'DID']
         if(i+j == len(inc) - 1):
            break
         j+=1
    
   inc["DID"] = inc.groupby("uuid")["DID"].rank("dense").astype(int)
   inc['DIDx'] = inc['DID']
   inc.drop(['index', 'username_country', 'DID_orig'], axis = 1, inplace = True)
   incremental_uniques = inc.copy()
   print("uniques algorithm finished in %d", datetime.now() - startTime)

   #appends the update date as the last column for auditing purposes and to be used by incremental refresh in PBI.
   incremental_uniques['updated_at'] = date_time
   incremental_uniques = incremental_uniques[['user_id', 'user_name', 'email', 'location', 'network_type', 'join_time', 'leave_time', 'leave_reason',
                                    'status', 'meeting_id', 'uuid', 'country', 'DID', 'DIDx', 'updated_at']]
   incremental_uniques = incremental_uniques.rename(columns={'location':'user_location', 'status':'user_status', 'email':'user_email'})
   #Save incremental data with unique IDs
   incremental_uniques.to_csv('./zoom_etl/'+date_time+'/fact_connections_uniques_'+date_time+'.csv', index= False)
   incremental_uniques.to_csv('./Production_Fact_Connections/fact_connections_uniques_'+date_time+'.csv', index= False)
   ########################################################################################################
   ## Incremental Algorithm Complete
   ########################################################################################################
   