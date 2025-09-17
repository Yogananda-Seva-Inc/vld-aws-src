#!/usr/bin/python

import os
import sys
import boto3

mysns = boto3.client('sns', region_name=os.getenv('AWS_DEFAULT_REGION'), aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))


mysns.publish(
             TopicArn = "arn:aws:sns:us-east-2:510649003322:omcprod",
             Message = "Hello Team,\nThe daily update is now available in S3.\nThank you.",
             Subject ="OMC Prod Daily Update Successful",
  )
