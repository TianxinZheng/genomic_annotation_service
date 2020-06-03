import uuid
import subprocess
from subprocess import PIPE
import os
import boto3
from botocore.exceptions import ClientError
import json

from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('ann_config.ini')

def update_job_status(job_id):
    dynamodb = boto3.resource('dynamodb', config['aws']['AwsRegionName'])
    ann_table = dynamodb.Table(config['aws']['AnnTableName'])
    try:
        response = ann_table.update_item(
            Key={
                'job_id': job_id
            },
            UpdateExpression="set job_status = :s",
            ConditionExpression="job_status = :p",
            ExpressionAttributeValues={
                ':s': "RUNNING",
                ':p': 'PENDING'
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
# Connect to SQS and get the message queue
sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'])
queue = sqs.get_queue_by_name(QueueName=config['aws']['SQSRequestQueueName'])
# Poll the message queue in a loop 
while True:
    # Attempt to read a message from the queue
    # Use long polling - DO NOT use sleep() to wait between polls
    messages = queue.receive_messages(WaitTimeSeconds=20)
    if (len(messages) > 0):
        message = messages[0]
        body = json.loads(message.body)['Message']
        info = json.loads(body)
        # If message read, extract job parameters from the message body as before
        job_id = info['job_id']
        user_id = info['user_id']
        input_file_name = info["input_file_name"]
        s3_inputs_bucket = info["s3_inputs_bucket"]
        s3_key_input_file = info["s3_key_input_file"]
        submit_time = info["submit_time"]
        job_status = info["job_status"]
        recipients = info["recipients"]
        user_role = info["user_role"]
        # Include below the same code you used in prior homework
        # Get the input file S3 object and copy it to a local file
        # Use a local directory structure that makes it easy to organize
        # multiple running annotation jobs
        s3 = boto3.resource('s3')
        key = s3_key_input_file
        print("key "+ key)
        new_dir = config['local']['LocalDir'] + user_id
        if not os.path.exists(new_dir):
            try: 
                os.mkdir(new_dir)
            except OSError as e:
                print(e.errno)
            except ValueError as e:
                print(e.errno)
            except Exception as e: 
                print(e.errno)
        
        try:
            file = s3.Bucket(s3_inputs_bucket).download_file(key, config['local']['LocalDir'] + key.split('/')[1] + '/' + key.split('/')[2])
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

        # Launch annotation job as a background process
        try:
            p2 = subprocess.Popen(['python', 'run.py', config['local']['LocalDir'] +  key.split('/')[1] + '/' + key.split('/')[2], job_id, recipients, user_role], stdout=PIPE, stderr=PIPE)
            update_job_status(job_id)
        except OSError as e:
            print(e.errno)
        except ValueError as e:
            print(e.errno)
        except Exception as e: 
            print(e.errno)
        # Delete the message from the queue, if job was successfully submitted
        try:
            delete_response = message.delete()
        except ClientError as e:
            print("Can't delete message.")




# reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html#GettingStarted.Python.03.03
#            https://docs.python.org/3/library/subprocess.html
#            https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/s3-example-download-file.html
#            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#client