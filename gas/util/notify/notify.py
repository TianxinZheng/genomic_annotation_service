# notify.py
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
import json
# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('notify_config.ini')

# Add utility code here

sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'])
queue = sqs.get_queue_by_name(QueueName=config['aws']['SQSQueueName'])

# Poll the message queue in a loop 
def go():
    while True:
        # Attempt to read a message from the queue
        # Use long polling - DO NOT use sleep() to wait between polls
        messages = queue.receive_messages(WaitTimeSeconds=20)
        if (len(messages) > 0):
            message = messages[0]
            body = json.loads(message.body)['Message']
            info = json.loads(body)
            # If message read, extract job parameters from the message body as before
            recipients = info['recipients']
            job_id = info['job_id']
            link = config['web']['JobDetailLink'] + job_id
            sender = config['email']['SenderEmail']
            subject = 'Your job ' + job_id + ' is completed!'
            body = subject + ' You can check the job detail at ' + link
            helpers.send_email_ses(recipients, sender, subject, body)
            try:
                delete_response = message.delete()
            except ClientError as e:
                print("Can't delete message.")

if __name__ == "__main__":
    go()
### EOF