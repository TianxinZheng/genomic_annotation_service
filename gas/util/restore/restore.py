# restore.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
from boto3.dynamodb.conditions import Key
# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers
import boto3
import json
from botocore.exceptions import ClientError
# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('restore_config.ini')

# Add utility code here
sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'])
restore_queue = sqs.get_queue_by_name(QueueName=config['aws']['SQSRestoreQueueName'])
dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
ann_table = dynamodb.Table(config['aws']["AnnTableName"])
glacier_client = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
# Poll the message queue in a loop 
while True:
    # Attempt to read a message from the queue
    # Use long polling - DO NOT use sleep() to wait between polls
    messages = restore_queue.receive_messages(WaitTimeSeconds=20)
    if (len(messages) > 0):
        message = messages[0]
        body = json.loads(message.body)['Message']
        info = json.loads(body)
        archive_ids = []
        user_id = info['user_id']

        #get all archive ids
        try:
            response = ann_table.query(
                IndexName='user_id_index',
                KeyConditionExpression=Key('user_id').eq(user_id)
            )
            for item in response['Items']:
                if 'results_file_archive_id' in item:
                    archive_ids.append(item['results_file_archive_id'])
        except ClientError as e:
            print("Query from dynamodb failed: " + str(e))

        #initiate restore 
        for archive_id in archive_ids:
            try:
                response = glacier_client.initiate_job(
                    vaultName=config['aws']['AwsGlacierVault'],
                    jobParameters={
                        'Type':"archive-retrieval",
                        'ArchiveId': archive_id,
                        'SNSTopic': config['aws']['SNSThawTopicArn'],
                        'Tier': "Expedited",
                    }
                )
            except ClientError as e:
                try:
                    response = glacier_client.initiate_job(
                        vaultName=config['aws']['AwsGlacierVault'],
                        jobParameters={
                            'Type':"archive-retrieval",
                            'ArchiveId': archive_id,
                            'SNSTopic': config['aws']['SNSThawTopicArn'],
                            'Tier': "Standard",
                        }
                    )
                    print(response)
                    
                       
                except ClientError as e:
                    print("Restore job request failed: " + str(e))
        try:
            delete_response = message.delete()
        except ClientError as e:
            print("Can't delete message.")





#reference:
#https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html
#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
#https://stackoverflow.com/questions/46950884/archive-retrieval-from-aws-glacier







### EOF