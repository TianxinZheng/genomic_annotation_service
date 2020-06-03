import boto3
import json
from botocore.exceptions import ClientError
import time

client = boto3.client('sns', region_name='us-east-1')
topic_arn = "arn:aws:sns:us-east-1:127134666975:txzheng_job_requests"
queue_data = { "job_id": "test_job_id", 
           "user_id": "test_user",
           "input_file_name": "test_input_file_name", 
           "s3_inputs_bucket": "gas-input",
           "s3_key_input_file": "test_key",
           "submit_time": 12713466,
           "job_status": "PENDING",
           "recipients": "fake@gmail.com",
           "user_role": "free_user"
         } 

while True:
    try:
        response = client.publish(
              TopicArn=topic_arn,
              Message = json.dumps(queue_data)
            )
        time.sleep(1)
    except ClientError as e:
        print("publish to sns failed")



