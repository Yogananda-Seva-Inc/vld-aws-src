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

def get_date():
    date_format='%Y-%m-%dT%H-%M-%SZ'
    date = datetime.now(tz=utc)
    date = date.astimezone(timezone('US/Pacific'))
    pstDateTime=date.strftime(date_format)
    date_time = pstDateTime
    os.environ['date_time'] = date_time
    
    return date_time

def reset_folders(date_time, root_dir = '.'):
    shutil.rmtree(os.path.join(root_dir,'zoom_etl'), ignore_errors=True)
    shutil.rmtree(os.path.join(root_dir,'Production_Fact_Connections'), ignore_errors=True)
    shutil.rmtree(os.path.join(root_dir,'Production_Fact_UUID'), ignore_errors=True)
    shutil.rmtree(os.path.join(root_dir,'Staging_Fact_UUID'), ignore_errors=True)
    shutil.rmtree(os.path.join(root_dir,'Staging_Fact_Connections'), ignore_errors=True)
    shutil.rmtree(os.path.join(root_dir,'Production_Service_Grouping'), ignore_errors=True)
    print('Deleted all previous folders with all data')

    createFolder(os.path.join(root_dir,'zoom_etl/'+date_time+''))
    createFolder(os.path.join(root_dir,'Production_Fact_Connections'))
    createFolder(os.path.join(root_dir,'Production_Fact_UUID'))
    createFolder(os.path.join(root_dir,'Staging_Fact_UUID'))
    createFolder(os.path.join(root_dir,'Staging_Fact_Connections'))
    createFolder(os.path.join(root_dir,'Production_Service_Grouping/RS'))
    createFolder(os.path.join(root_dir,'Production_Service_Grouping/SP'))
    createFolder(os.path.join(root_dir,'Production_Service_Grouping/PS'))
    createFolder(os.path.join(root_dir,'Production_Service_Grouping/RSEG'))

    return

if __name__ == "__main__":
    date_time = get_date()
    reset_folders(date_time)