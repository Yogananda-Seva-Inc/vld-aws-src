#pip install pyzoom
import pandas as pd
from pyzoom import ZoomClient

client = ZoomClient('vqnbcob8TNyyqU3mciyMPQ', 'nLB3CojR37rjLAL0nwLFWp6mJYli55th5pNZ')

from urllib.parse import urlencode, quote_plus

def testcase(text, result):
   #Test to see if the double_urlencode function's output for an input of 'text' matches 'result'
   text = double_urlencode(text)

   if (result != text):
       print("Error: double_urlencode testcase failure :(")


       # Pretty up the problem for debugging purposes
       print(text)
       print(result)

       buf = ""
       for i in range(0, len(result)):
           try:
               if text[i] == result[i]:
                   buf += " "
               else:
                   buf += "^"
           except IndexError:
               buf += "*"

       print(buf)


def double_urlencode(text):
   #double URL-encode a given 'text'.  Do not return the 'variablename=' portion

   text = single_urlencode(text)
   text = single_urlencode(text)

   return text

def single_urlencode(text):
   #single URL-encode a given 'text'.  Do not return the 'variablename=' portion

   blah = urlencode({'blahblahblah':text})

   #we know the length of the 'blahblahblah=' is equal to 13.  This lets us avoid any messy string matches
   blah = blah[13:]

   return blah

df_meetingids = pd.read_csv('service_listing_meeting_ids.csv')

df_uuids = pd.DataFrame()
count = 0
for i in range(0, len(df_meetingids)):
    meeting_id = df_meetingids.loc[i, 'MeetingID']
    zoom_data = client.raw.get_all_pages('/past_meetings/'+str(meeting_id)+'/instances')
    df_meeting_uuids = pd.DataFrame(zoom_data['meetings'])
    for i in range(0, len(df_meeting_uuids)):
        meeting_uuid = df_meeting_uuids.loc[i, 'uuid']
        try:
            zoom_data = client.raw.get_all_pages('/past_meetings/'+double_urlencode(str(meeting_uuid))+'')
            df_uuids_temp = pd.DataFrame(zoom_data, index=[0])
            df_uuids = pd.concat([df_uuids,df_uuids_temp])
            count = count + 1
            print('UUIDs for Meeting ID ',meeting_id, 'and UUID: ', meeting_uuid, 'is COMPLETED', 'Count :', count)
        except:
            count = count + 1
            print('UUIDs for Meeting ID ',meeting_id, 'and UUID: ', meeting_uuid, 'is NOT_COMPLETED', 'Count :', count)
            continue

df_uuids.to_csv('new_fact_uuid_file.csv', index=False)