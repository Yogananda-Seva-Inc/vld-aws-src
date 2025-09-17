import glob,os,sys
os.system(f'{sys.executable} -m pip install -r requirements.txt')
for script in ['UploadtoS3.py','UploadtoS3_FactConnection.py','UploadtoS3_FactUUID.py']:
    with open(script) as f:
       contents = f.read()
    exec(contents)
