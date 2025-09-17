import glob,os,sys
os.system(f'{sys.executable} -m pip install -r requirements.txt')
for script in ['SNSPublish.py']:
    with open(script) as f:
       contents = f.read()
    exec(contents)
