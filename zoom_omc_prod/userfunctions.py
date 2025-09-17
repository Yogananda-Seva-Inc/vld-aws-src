from requests.utils import requote_uri
from urllib.parse import urlencode, quote_plus, quote
import datetime
import requests
import pandas as pd
from pytz import timezone
import pytz

def single_urlencode(text):
   #single URL-encode a given 'text'.  Do not return the 'variablename=' portion
   blah = urlencode({'blahblahblah':text})
   #we know the length of the 'blahblahblah=' is equal to 13.  This lets us avoid any messy string matches
   blah = blah[13:]
   return blah

def double_urlencode(text):
   #double URL-encode a given 'text'.  Do not return the 'variablename=' portion
   text = single_urlencode(text)
   text = single_urlencode(text)
   return text

def encode_uuid(val):
    """Encode UUID as described by ZOOM API documentation
    > Note: Please double encode your UUID when using this API if the UUID
    > begins with a '/'or contains ‘//’ in it.
    :param val: The UUID to encode
    :returns: The encoded UUID
    """
    if val[0] == "/" or "//" in val:
        val = quote(quote(val, safe=""), safe="")
    return val

def convert_to_pst(a):
    date_format='%Y-%m-%d %H:%M:%S'
    date = datetime.datetime.strptime(a, '%Y-%m-%dT%H:%M:%S.%fZ')
    my_timezone = timezone('US/Pacific')
    date = date.astimezone(my_timezone)
    return date.strftime(date_format)

def uniq_algorithm(inc):
    common_list = ["user_name",  "iPhone", "iPad", "Jai Guru",  "Yogananda", "John", "Jane", "Mary",  "Maria", "Paul",  "Redmi"]
    
    inc['location']=inc.location.apply(str)
    inc['country']=inc['location'].apply(lambda st: st[st.find("(")+1:st.find(")")])
    inc.dropna(subset=['country'], inplace=True)
    inc.reset_index(drop=True, inplace=True)
    for i in range(0, len(inc)-1):
      print(i)
      print(inc.iloc[i, 2])
      if(inc.loc[i, 'user_name'] in common_list):
        inc.loc[i, 'user_name'] = inc.loc[i, 'location']
      print("isnull")
      if(pd.isnull(inc.loc[i, 'user_name'])):
        inc.loc[i, 'username_country'] = str(inc.loc[i, 'country'])
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
    return inc