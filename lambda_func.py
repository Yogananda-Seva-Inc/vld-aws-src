import json

def lambda_handler(event, context):
    # TODO: implement from console
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda! updated from github ')
    }
