# thaw.py
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
config.read('thaw_config.ini')

# Add utility code here
sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'])
queue = sqs.get_queue_by_name(QueueName=config['aws']['SQSThawQueueName'])
s3 = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
ann_table = dynamodb.Table(config['aws']["AnnTableName"])
glacier = boto3.resource('glacier', region_name=config['aws']['AwsRegionName'])
while True:
    # Attempt to read a message from the queue
    # Use long polling - DO NOT use sleep() to wait between polls
    messages = queue.receive_messages(WaitTimeSeconds=20)
    if (len(messages) > 0):
        message = messages[0]
        body = json.loads(message.body)['Message']
        info = json.loads(body)
        retrieval_job_id = info["JobId"]
        archive_id = info['ArchiveId']
        job = glacier.Job('-', config['aws']['AwsGlacierVault'], retrieval_job_id)
        
        #get job output
        try: 
            response = job.get_output()
            job_data = response['body']
        except ClientError as e:
            print("Get job output failed: " + str(e)) 
        
        #get the s3 key to upload to
        print("archive_id " + archive_id)
        try:
            response = ann_table.query(
                IndexName='results_file_archive_id_index',
                KeyConditionExpression=Key('results_file_archive_id').eq(archive_id)
            )
            
        except ClientError as e:
            print("Query from dynamodb failed: " + str(e))
        print("response" + str(response))
        item = response['Items'][0]
        s3_key_result_file = item['s3_key_result_file']

        #upload job output to s3 bucket
        try:
            response = s3.upload_fileobj(job_data, config['aws']['ResultBucketName'], s3_key_result_file)
        except ClientError as e:
            print("Can't upload restored file to S3.")

        #delete file from glacier
        glacier_client = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
        try:
            response = glacier_client.delete_archive(
                vaultName=config['aws']['AwsGlacierVault'],
                archiveId=archive_id
            )
        except ClientError as e:
            print("Can't delete archive from glacier.")

        #delete message
        try:
            delete_response = message.delete()
        except ClientError as e:
            print("Can't delete message.")
       


#reference:
#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Job.get_output
#https://docs.aws.amazon.com/amazonglacier/latest/dev/api-initiate-job-post.html
#https://docs.aws.amazon.com/amazonglacier/latest/dev/deleting-an-archive.html
### EOF