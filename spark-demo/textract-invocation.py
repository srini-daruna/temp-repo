#  Copyright Amazon.com Inc. or its affiliates.

import json
import boto3
import uuid
from datetime import datetime

def lambda_handler(event, context):
    resource = boto3.resource('s3')
    textract_client = boto3.client('textract')
    dynamodb = boto3.resource('dynamodb',"us-east-1").Table("textract-job-details")
    
    ## Get the object name
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    ## ClientRequestToken should be unique when calling textract
    unique_hash_for_client_request_token = uuid.uuid4().hex
    ## Invoke textract start analysis and store the record in DynamoDB
    response = textract_client.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        },
        ClientRequestToken=unique_hash_for_client_request_token,
        NotificationChannel={
            'SNSTopicArn': 'arn:aws:sns:us-east-1:230862563692:textract-job-notification',
            'RoleArn': 'arn:aws:iam::230862563692:role/role-for-blog'
        })
    
    print("textract document text detection completed. Job id is {} and path is s://{}".format(response['JobId'], bucket+key))
    print("submission time is {} and data type of date field is {}".format(response['ResponseMetadata']['HTTPHeaders']['date'],type(response['ResponseMetadata']['HTTPHeaders']['date'])))
    
    submission_time = response['ResponseMetadata']['HTTPHeaders']['date']
    
    db_response = dynamodb.put_item(
        Item={'file_path': "s3://{}/{}".format(bucket,key),
              'job_id': response['JobId'],
              'job_status': 'SUBMITTED',
              'submission_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
             })
             
    print("record has been inserted into dynamodb table {}".format("textract-job-details"))
    
    return {
        'statusCode': 200,
        'body': json.dumps(db_response)
    }
