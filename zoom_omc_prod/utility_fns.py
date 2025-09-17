from datetime import datetime
from pytz import timezone, utc
import os
import shutil

def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)

date_format='%Y-%m-%dT%H:%M:%SZ'
date = datetime.now(tz=utc)
date = date.astimezone(timezone('US/Pacific'))
pstDateTime=date.strftime(date_format)
date_time = pstDateTime
os.environ['date_time'] = date_time
# variables['date_time'] = date_time

#dbx_token = variables['dbx_token']
#dbx = dropbox.Dropbox(dbx_token)

shutil.rmtree('./zoom_etl', ignore_errors=True)
shutil.rmtree('./Production_Fact_Connections', ignore_errors=True)
shutil.rmtree('./Production_Fact_UUID', ignore_errors=True)
shutil.rmtree('./Production_Service_Grouping', ignore_errors=True)
print('Deleted all previous folders with all data')

createFolder('./zoom_etl/'+date_time+'')
createFolder('./Production_Fact_Connections')
createFolder('./Production_Fact_UUID')
createFolder('./Production_Service_Grouping/RS')
createFolder('./Production_Service_Grouping/SP')
createFolder('./Production_Service_Grouping/PS')
createFolder('./Production_Service_Grouping/RSEG')
# dropboxNewSubDir = '/'+date_time+''

# res = dbx.files_create_folder_v2(dropboxNewSubDir)
# access the information for the newly created folder in `res`
