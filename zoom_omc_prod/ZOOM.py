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


if __name__ == "__main__":

    df_uuid = pd.read_csv('new_fact_uuid_file.csv')
    count = 0
    df_master_data = pd.DataFrame()
    for i in range(0, len(df_uuid)):
        zoom_data = client.raw.get_all_pages('metrics/meetings/'+double_urlencode(str(df_uuid.loc[i, 'uuid']))+'/participants")
        df_participants = pd.DataFrame(zoom_data)
        df_participants['meeting_id'] = df_uuid.loc[i, 'id']
        df_participants['uuid'] = df_uuid.loc[i, 'uuid']
        df_master_data = pd.concat([df_master_data,df_participants])
        df_uuid.loc[i, 'etl_status'] = 'Y'
        count = count + 1
        print('ETL for Meeting ID: ',df_uuid.iloc[i]['id'], 'and UUID: ', df_uuid.loc[i, 'uuid'], 'is completed and Count : ',count)
    df_uuid = df_uuid.reset_index(drop=True)
    df_uuid.to_csv('uuid_etl.csv', index=False)
    df_master_data = df_master_data.reset_index(drop=True)
    df_master_data.to_csv('fact_connections.csv', index= False)