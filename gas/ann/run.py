#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import os
import time
import driver
import boto3
from botocore.exceptions import ClientError
import time
import json
"""A rudimentary timer for coarse-grained profiling
"""
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('ann_config.ini')

def update_job_status(job_id, log_object, annot_object):
  dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
  ann_table = dynamodb.Table(config['aws']['AnnTableName'])
  complete_time = int(time.time())
  try:
      response = ann_table.update_item(
          Key={
              'job_id': job_id
          },
          UpdateExpression="set job_status = :s, s3_key_result_file = :r,\
           s3_key_log_file = :l, s3_results_bucket = :t, complete_time = :c",
          ConditionExpression="job_status = :p",
          ExpressionAttributeValues={
              ':s': "COMPLETED",
              ':r': annot_object,
              ':l': log_object,
              ':t': 'gas_results',
              ':c': complete_time,
              ':p': 'RUNNING'
          },
          ReturnValues="UPDATED_NEW"
      )
  except ClientError as e:
      if e.response['Error']['Code'] == "ConditionalCheckFailedException":
          print(e.response['Error']['Message'])
      else:
          raise
  else:
      print("UpdateItem succeeded:")

def publish_to_results_sns(job_id, recipients):
  client = boto3.client('sns', region_name=config['aws']['AwsRegionName'])
  topic_arn = config['aws']['SNSResultsTopicArn']   
  #sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'])
  #queue = sqs.get_queue_by_name(QueueName=config['aws']['SQSQueueName'])
  data = {
      "job_id": job_id, 
      "recipients": recipients 
  }
  try:
    response = client.publish(
      TopicArn=topic_arn,
      Message = json.dumps(data)
    )
  except ClientError as e:
    print("publish to sns results topic failed")

def publish_to_archive_sns(job_id):
  client = boto3.client('sns', region_name=config['aws']['AwsRegionName'])
  topic_arn = config['aws']['SNSArchiveTopicArn']   
  data = {
      "job_id": job_id
  }
  try:
    response = client.publish(
      TopicArn=topic_arn,
      Message = json.dumps(data)
    )
  except ClientError as e:
    print("publish to sns archive topic failed")

class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
      print(f"Approximate runtime: {self.secs:.2f} seconds")

if __name__ == '__main__':
  # Call the AnnTools pipeline
  if len(sys.argv) > 1:
    with Timer():
      print("sys.argv[1] " + sys.argv[1])
      driver.run(sys.argv[1], 'vcf')
      s3_client = boto3.client('s3')
      bucket = config['aws']['ResultBucketName']
      log_file = sys.argv[1] + '.count.log'
      log_object = sys.argv[1][5:] + '.count.log'
      annot_file = sys.argv[1][:len(sys.argv[1]) - 4] + '.annot.vcf'
      annot_object = sys.argv[1][5:len(sys.argv[1]) - 4] + '.annot.vcf'
      job_id = sys.argv[2]
      recipients = sys.argv[3]
      user_role = sys.argv[4]
      #print("log_file " + log_file)
      #print("annot_file " + annot_file)
      #print("log_object "+ log_object)
      #print("annot_object "+ annot_object)

      try:
        response = s3_client.upload_file(log_file, bucket, log_object)
      except ClientError as e:
        print("Can't upload log file to S3.")

      try:
        response = s3_client.upload_file(annot_file, bucket, annot_object)
      except ClientError as e:
        print("Can't upload annotated file to S3.")
      
      try:
        os.remove(sys.argv[1])
      except:
        print("Error while deleting file ", sys.argv[1])

      try:
        os.remove(log_file)
      except:
        print("Error while deleting file ", log_file)

      try:
        os.remove(annot_file)
      except:
        print("Error while deleting file ", annot_file)
      
      update_job_status(job_id, log_object, annot_object)
      
      publish_to_results_sns(job_id, recipients)
      if user_role == "free_user":
        publish_to_archive_sns(job_id)

  else:
    print("A valid .vcf file must be provided as input to this program.")

### EOF