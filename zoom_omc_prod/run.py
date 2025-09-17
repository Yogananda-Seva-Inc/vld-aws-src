import glob,os,sys
#os.system(f'{sys.executable} -m pip install -r requirements.txt')
for script in ['userfunctions.py', 'utility_fns.py', 'ZoomDownloadv3.py', 'UploadtoS3.py','UploadtoS3_FactConnection.py','UploadtoS3_FactUUID.py', 'SNSPublish.py']:
    with open(script) as f:
       contents = f.read()
    exec(contents)
