import boto3
import time
import sys
import os
job_name = 'prod_rds_to_rds_factconnection'
client = boto3.client(service_name='glue', region_name=os.getenv('AWS_DEFAULT_REGION'),
                      endpoint_url='https://glue.us-east-2.amazonaws.com')
response = client.start_job_run(JobName=job_name)
status = client.get_job_run(JobName=job_name, RunId=response['JobRunId'])
print('Glue Job ' + job_name + ' Running', flush=True)

if status:
    state = status['JobRun']['JobRunState']
    while state not in ['SUCCEEDED']:
        time.sleep(30)
        status = client.get_job_run(
            JobName=job_name, RunId=response['JobRunId'])
        state = status['JobRun']['JobRunState']
        if state in ['STOPPED', 'FAILED', 'TIMEOUT']:
            raise Exception('Failed to execute glue job: ' +
                            status['JobRun']['ErrorMessage'] + '. State is : ' + state)
        elif state in ['SUCCEEDED']:
            print('Glue Job ' + job_name + ' Executed', flush=True)
            break
