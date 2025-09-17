import arrow
from zoomus import ZoomClient as RawClient
import pandas as pd
import json
from userfunctions import single_urlencode, double_urlencode, encode_uuid, convert_to_pst, uniq_algorithm
from pyzoom import ZoomClient as PyZoomClient

client = RawClient('WLbxTW1PRga58pfa_OZfXQ','qSIYrdmrBqV0TqZMjuxzzANgH18eIVcP','cSSQ81HPQYqh7oMtBMJgAA')
print('client : ', client.api_secret)
print('client : ', client.config["token"])

# dfs_meeting_instances = []
# for i in range(0,30,1):
#   start_date = arrow.now().shift(days=-i-1).format('YYYY-MM-DD')
#   end_date = arrow.now().shift(days=-i).format('YYYY-MM-DD')
#   print(start_date)
#   print(end_date)
#   query_param_list_meetings = {
#     "from": start_date,
#     "to": end_date,
#     "type": "past",
#     "page_size": 300
#   }
#   list_meetings_response = client.get_request('/metrics/meetings',query_param_list_meetings)
#   list_meetings_content =  json.loads(list_meetings_response.content)
#   df_name = 'df_meeting_intsances'+str(int(i))
#   df_name = pd.DataFrame(list_meetings_content['meetings'])
#   dfs_meeting_instances.append(df_name)
#   print('df_name shape:', df_name.shape)

# df_meeting_instances = pd.concat(dfs_meeting_instances, axis=0)
# print('df_meeting_instances shape:', df_meeting_instances.shape)
# df_meeting_instances = df_meeting_instances.drop_duplicates().reset_index()
# print('df_meeting_instances shape:', df_meeting_instances.shape)


# query_param_meeting_participants = {
#   "page_size": 200
# }
# meeting_participants_flag = True
# df_participants = []

# while(meeting_participants_flag):
#   past_meeting_partcipants_response = client.get_request('/past_meetings/'+encode_uuid('/015NxG2QUqBesDofTa9Dw==')+'/participants', query_param_meeting_participants)
#   past_participants_content =  json.loads(past_meeting_partcipants_response.content)
#   next_page_token = past_participants_content['next_page_token']
#   df_participants = pd.DataFrame(past_participants_content['participants'])
#   print('df_participants shape:', df_participants.shape)
#   meeting_participants_flag = False


PyClient = PyZoomClient(client.config["token"])
uuid = encode_uuid('/015NxG2QUqBesDofTa9Dw==')
#result_dict = PyClient.raw.get_all_pages('/past_meetings/'+encode_uuid('/015NxG2QUqBesDofTa9Dw==')+'/participants/qos')
result_dict = PyClient.raw.get_all_pages('/metrics/meetings/'+uuid+'/participants?type=past')
df_participants = pd.DataFrame(result_dict['participants'])
print('df_participants shape:', df_participants.shape)
print('df_participants columns:', df_participants.columns)

# result_dict_m = PyClient.raw.get_all_pages('/metrics/meetings',query_param_list_meetings)
# print(result_dict_m)
#print('next_page_token', result_dict_m['next_page_token'])
#df_meetings = pd.DataFrame(result_dict_m['meetings'])
#print('df_meetings shape:', df_meetings.shape)
client = RawClient('WLbxTW1PRga58pfa_OZfXQ','qSIYrdmrBqV0TqZMjuxzzANgH18eIVcP','cSSQ81HPQYqh7oMtBMJgAA')
print('client : ', client.api_secret)
print('client : ', client.config["token"])
start_date = arrow.now().shift(days=-28).format('YYYY-MM-DD')
end_date = arrow.now().format('YYYY-MM-DD')
print(start_date)
print(end_date)
query_param_list_meetings = {
  "from": start_date,
  "to": end_date
}
# list_meetings_response = client.get_request('/metrics/meetings?type=past',query_param_list_meetings)
# list_meetings_content =  json.loads(list_meetings_response.content)
list_meetings_content = PyClient.raw.get_all_pages('/metrics/meetings?type=past&page_size=300&from='
                                                   +start_date+'&to='+end_date)
df_meeting_instances = pd.DataFrame(list_meetings_content['meetings'])
print('df_meeting_instances shape:', df_meeting_instances.shape)
print('df_meeting_instances columns:', df_meeting_instances.columns)
df_meeting_instances = df_meeting_instances.drop_duplicates().reset_index()
print('df_meeting_instances shape:', df_meeting_instances.shape)
