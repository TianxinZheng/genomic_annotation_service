# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime


import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template, 
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3', 
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']
  profile = get_profile(identity_id=session.get('primary_identity'))

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = request.args.get('bucket')
  key = request.args.get('key')
  

  # Extract the job ID from the S3 key
  whole_file_name = key.split('/')[2]
  job_id = whole_file_name.split("~")[0]
  input_file_name = whole_file_name.split("~")[1]
  submit_time = int(time.time())
  user_id = session['primary_identity']
  recipients = get_profile(identity_id=user_id).email
  user_role = get_profile(identity_id=user_id).role
  # Persist job to database
  # Move your code here...
  db_data = { "job_id": job_id, 
           "user_id": user_id,
           "input_file_name": input_file_name, 
           "s3_inputs_bucket": bucket_name,
           "s3_key_input_file": key,
           "submit_time": submit_time,
           "job_status": "PENDING"
         }    
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  try:
    ann_table.put_item(Item=db_data)
  except ClientError as e:
    app.logger.error(f"Failed to put item into database: {e}")
  # Send message to request queue
  # Move your code here...
  client = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
  topic_arn = app.config['AWS_SNS_JOB_REQUEST_TOPIC']
  queue_data = { "job_id": job_id, 
           "user_id": user_id,
           "input_file_name": input_file_name, 
           "s3_inputs_bucket": bucket_name,
           "s3_key_input_file": key,
           "submit_time": submit_time,
           "job_status": "PENDING",
           "recipients": recipients,
           "user_role": user_role
         } 
  try:
    response = client.publish(
      TopicArn=topic_arn,
      Message = json.dumps(queue_data)
    )
  except ClientError as e:
    print("publish to sns failed")

  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  user_id = session['primary_identity']
  print(user_id)
  # Get list of annotations to display
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  annotations = []
  try:
    response = ann_table.query(
      IndexName='user_id_index',
      KeyConditionExpression=Key('user_id').eq(user_id)
    )
    
    for item in response['Items']:
      annotation = {}
      job_id = item["job_id"]
      submit_time = item["submit_time"]
      submit_time = datetime.fromtimestamp(submit_time).strftime('%Y-%m-%d %H:%M')
      input_file_name = item["input_file_name"]
      job_status = item["job_status"]

      annotation["job_id"] = job_id
      annotation["submit_time"] = submit_time
      annotation["input_file_name"] = input_file_name
      annotation["job_status"] = job_status
      annotations.append(annotation)
  except ClientError as e:
    print("query from dynamodb failed " + str(e))
  return render_template('annotations.html', annotations=annotations)


"""Display details of a specific annotation job
"""

@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  s3 = boto3.client('s3', 
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))
  user_id = session['primary_identity']
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  try:
    response = ann_table.query(
      KeyConditionExpression=Key('job_id').eq(id)
    )
    annotation = {}
    item = response['Items'][0]
    job_id = item["job_id"]
    submit_time = item["submit_time"]
    submit_time = datetime.fromtimestamp(submit_time).strftime('%Y-%m-%d %H:%M')
    input_file_name = item["input_file_name"]
    job_status = item["job_status"]
    
    annotation["job_id"] = job_id
    annotation["submit_time"] = submit_time
    annotation["input_file_name"] = input_file_name
    annotation["job_status"] = job_status

    input_key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + job_id + '~' + input_file_name
    try:
      url = s3.generate_presigned_url('get_object',
                                      Params={'Bucket': app.config['AWS_S3_INPUTS_BUCKET'],
                                              'Key': input_key_name},
                                              ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    except ClientError as e: 
      print("Failed to generate presigned url for downloading the input file: " + str(e))                                             
   
    annotation["input_file_url"] = url
    if 'complete_time' in item:
      complete_time = item['complete_time']
      cur_time = int(time.time())
      annotation["user_role"] = session.get('role')
      annotation["free_access_expired"] = cur_time - complete_time > 300
      annotation['complete_time'] = datetime.fromtimestamp(complete_time).strftime('%Y-%m-%d %H:%M')
      results_key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + job_id + '~' +\
                        input_file_name.split('.')[0] + '.' + app.config['RESULTS_SUFFIX'] + '.' +\
                        input_file_name.split('.')[1]
      
      #annotation['restore_message'] = 
      #check if the result file in s3 bucket
      if annotation["user_role"] == "premium_user":
        try:
          s3_resource = boto3.resource('s3')
          s3_resource.Object(app.config['AWS_S3_RESULTS_BUCKET'], results_key_name).load()
        except ClientError as e:
          annotation['restore_message'] = "Your result file is being restored. Please check back after 4 ~ 5 hours."
        try:
          url = s3.generate_presigned_url('get_object',
                                          Params={'Bucket': app.config['AWS_S3_RESULTS_BUCKET'],
                                                  'Key': results_key_name},
                                                  ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
          annotation["result_file_url"] = url
        except ClientError as e: 
          print("Failed to generate presigned url for downloading the result file: " + str(e)) 
      elif annotation["user_role"] == "free_user" and not annotation["free_access_expired"]:
        try:
          url = s3.generate_presigned_url('get_object',
                                          Params={'Bucket': app.config['AWS_S3_RESULTS_BUCKET'],
                                                  'Key': results_key_name},
                                                  ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
          annotation["result_file_url"] = url
        except ClientError as e: 
          print("Failed to generate presigned url for downloading the result file: " + str(e)) 
  except ClientError as e:
    print("Query from dynamodb failed: " + str(e))
  #print("annotation" + str(annotation))
  return render_template('annotation_details.html', annotation=annotation)
 

"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  job_id = id
  user_id = session['primary_identity']
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  try:
    response = ann_table.query(
      KeyConditionExpression=Key('job_id').eq(id)
    )
    item = response['Items'][0]
    input_file_name = item['input_file_name']
  except ClientError as e:
    print("Query from dynamodb failed: " + str(e))
  s3 = boto3.resource('s3', 
    region_name=app.config['AWS_REGION_NAME'])
  log_key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + job_id + '~' +\
                 input_file_name + app.config['LOG_SUFFIX']
  try:
    obj = s3.Object(app.config['AWS_S3_RESULTS_BUCKET'], log_key_name)
    log_file_contents = obj.get()['Body'].read().decode('utf-8')

  except ClientError as e: 
    print("Failed to generate presigned url for downloading the result file: " + str(e))  
  return render_template('view_log.html', job_id=job_id, log_file_contents=log_file_contents)


"""Subscription management handler
"""
import stripe

@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Process the subscription request
    token = str(request.form['stripe_token']).strip()

    # Create a customer on Stripe
    stripe.api_key = app.config['STRIPE_SECRET_KEY']
    try:
      customer = stripe.Customer.create(
        card = token,
        plan = "premium_plan",
        email = session.get('email'),
        description = session.get('name')
      )
    except Exception as e:
      app.logger.error(f"Failed to create customer billing record: {e}")
      return abort(500)

    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!
    client = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
    restore_topic_arn = app.config['AWS_SNS_RESTORE_TOPIC'] 
    data = {
      "user_id": session['primary_identity']
    }
    try:
      response = client.publish(
        TopicArn=restore_topic_arn,
        Message = json.dumps(data)
      )
    except ClientError as e:
      print("publish to sns results topic failed") 
    # Display confirmation page
    return render_template('subscribe_confirm.html', 
      stripe_customer_id=str(customer['id']))


"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500


#reference:
#https://stackoverflow.com/questions/12400256/converting-epoch-time-into-the-datetime
#https://stackoverflow.com/questions/35758924/how-do-we-query-on-a-secondary-index-of-dynamodb-using-boto3
#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_url
#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.get
### EOF