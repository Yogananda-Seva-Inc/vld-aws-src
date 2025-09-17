# -*- coding: utf-8 -*-
import os
import shutil
import chardet
import sys
import logging
import json
import math
import csv
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
from .userfunctions import convert_to_pst, uniq_algorithm
import urllib3

pd.options.mode.chained_assignment = None

if __name__ == "__main__":
    incremental = pd.read_csv('./zoom_etl/'+variables['date_time']+'/fact_connections.csv')
    print(incremental.shape)

    # Converts UTC join_time and leave_time to PT timezone. Also removes missing join_time, leave_time, ip_address, meeting_id rows.
    incremental = incremental.dropna(how='all')
    incremental = incremental.dropna(axis=0, subset=['join_time', 'leave_time', 'ip_address', 'meeting_id'])
    incremental['join_time'] = incremental.apply(lambda x: convert_to_pst(x['join_time']), axis = 1)
    incremental['leave_time'] = incremental.apply(lambda x: convert_to_pst(x['leave_time']), axis = 1)
    # Create incremental index and sort by uuid and DID_Orig. Sorting by these variables is required to run the Uniques Algorithm successfully.

    print("running uniques algorithm for incremental data..")
    startTime = datetime.now()
    incremental_uniques = uniq_algoritm(incremental)
    print("uniques algorithm finished in %d", datetime.now() - startTime)

    #appends the update date as the last column for auditing purposes and to be used by incremental refresh in PBI.
    incremental_uniques['updated_at'] = max(pd.to_datetime(incremental_uniques['join_time']).dt.date)

    #Save incremental data with unique IDs
    incremental_uniques.to_csv('./zoom_etl/'+variables['date_time']+'/fact_connections_master.csv', index= False)