import glob
import os
import sys
#os.system(f'{sys.executable} -m pip install -r requirements.txt')

for script in ['user_functions.py', 'utility_functions.py', 'zoom_download.py', \
               's3_upload.py','s3_upload_fact_conn.py','s3_upload_fact_uuid.py',
               's3_upload_staging_fact_uuid.py','s3_upload_staging_fact_conn.py']:
               #'prod_s3_to_rds_factconnection_glue.py','prod_s3_to_rds_factuuid_glue.py','prod_rds_to_rds_factuuid_glue.py','prod_rds_to_rds_factconnection_glue.py','SNSPublish.py']:
    with open(script) as f:
        print(script)
        contents = f.read()
    exec(contents)