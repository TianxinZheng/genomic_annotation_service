# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import boto3
# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers
import json
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('archive_config.ini')

client = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'])
queue = sqs.get_queue_by_name(QueueName=config['aws']['SQSArchiveQueueName'])
dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
ann_table = dynamodb.Table(config['aws']['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
s3 = boto3.resource('s3', region_name=config['aws']['AwsRegionName'])
# Add utility code here
while True:
    # Attempt to read a message from the queue
    # Use long polling - DO NOT use sleep() to wait between polls
    messages = queue.receive_messages(WaitTimeSeconds=20)
    if (len(messages) > 0):
        print("test")
        message = messages[0]
        body = json.loads(message.body)['Message']
        info = json.loads(body)
        job_id = info['job_id']
        #get the result file name from dynamodb
        try:
            response = ann_table.query(
              KeyConditionExpression=Key('job_id').eq(job_id)
            )
            item = response['Items'][0]
            result_file_name = item['s3_key_result_file']
        except ClientError as e:
            print("Query from dynamodb failed: " + str(e))

        #get the file from results bucket
        try:
            obj = s3.Object(config['aws']['AWS_S3_RESULTS_BUCKET'], result_file_name)
            file = obj.get()['Body'].read()

        except ClientError as e: 
            print("Failed to generate presigned url for downloading the result file: " + str(e)) 

        #upoload the file to Glacier
        try:   
            response = client.upload_archive(
                vaultName=config['aws']['GlacierVaultName'],
                body=file
            )
            archive_id = response['archiveId']
        except ClientError as e: 
            print("Failed to upload the result file to Glacier: " + str(e))
        
        #update the Glacier archive id in dynamodb
        try:
            response = ann_table.update_item(
                Key={
                    'job_id': job_id
                },
                UpdateExpression="set results_file_archive_id = :archive_id",
                ConditionExpression="job_id = :job_id",
                ExpressionAttributeValues={
                    ':archive_id': archive_id,
                    ':job_id': job_id
                },
                ReturnValues="UPDATED_NEW"
            )
        except ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                print(e.response['Error']['Message'])
            else:
                raise
        else:
            print("UpdateItem succeeded.")

        #delete the file from s3 bucket
        try:
            obj.delete()
        except ClientError as e: 
            print("Failed to delete the result file in S3 bucket: " + str(e)) 

        #delete the message
        try:
            delete_response = message.delete()
        except ClientError as e:
            print("Can't delete message.")
#reference:
#https://stackoverflow.com/questions/41833565/s3-buckets-to-glacier-on-demand-is-it-possible-from-boto3-api
#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.upload_archive
#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object
### EOF