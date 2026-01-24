#!/usr/bin/python                                                                                                                                           import os
import sys
import boto3
import os
mysns = boto3.client('sns', region_name=os.getenv('AWS_DEFAULT_REGION'))#, aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
mysns.publish(
    TopicArn="arn:aws:sns:us-east-2:510649003322:omcaws",
    Message="Hello Team,\nThe daily update is now available in RDS.\nThank you.",
    Subject="OMC AWS Daily Update Successful",
)
print('Data Update EMAIL Sent')
