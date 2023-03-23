import json
import boto3
import email
import os
from datetime import datetime
import re

def lambda_handler(event, context):
    
    # Initiate boto3 client
    s3 = boto3.client('s3')

    # Retrieve bucket name and object key
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = event['Records'][0]['s3']['object']['key']

    # Get s3 object contents based on bucket name and object key
    s3_response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    file_content = s3_response['Body'].read()

    # Use the email library to retrieve the the s3 object content coming from SES (email, message content and attachment)
    msg = email.message_from_string(file_content.decode('utf-8'))
    attachment = msg.get_payload()[1]
    file_name = attachment.get_filename()
    regex_getAirline = r"(\d{4}_\d{2}_\d{2})_(\w+)\.csv"
    airline_name = re.findall(regex_getAirline, file_name)[0][1]
    key_name = attachment.get_filename()[:10]

    # Write the attachment to a temp location
    temp_file = '/tmp/' + file_name

    with open (temp_file, 'wb') as f:
        f.write(attachment.get_payload(decode=True))

    # Construct the destination key with the airline name, file name and original file extension
    destination_key = airline_name + '/' + key_name + '.csv'

    # Upload the file to the destination bucket 
    try:
        s3.upload_file(temp_file, 'airline-email-extract-attachment', destination_key)
        print("Upload Successful")
    except FileNotFoundError:
        print("The file was not found")

    # Clean up the file from temp location
    os.remove(temp_file)

    return {
        'statusCode': 200,
        'body': json.dumps('SES email received and attachment processed!')
    }
