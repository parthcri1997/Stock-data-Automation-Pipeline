import json
import boto3
import time

def lambda_handler(event, context):
    glue_client = boto3.client('glue', region_name='us-east-1')
    for record in event['Records']:
        print("Event:", event)
        print("Record:",record)
        bucket_name = record['s3']['bucket']['name']
        file_key = record['s3']['object']['key']
        # bucket_name = event['Records'][0]['s3']['bucket']['name']
        # file_key = event['Records'][0]['s3']['object']['key']
        job_name = 'etl_glue_job_using_cf'
        file_key_dest = 'csv/'
        print("Event:", event)
        print(file_key)
        print(bucket_name)
        response = glue_client.start_job_run(JobName=job_name, Arguments={
            '--job_name': job_name,
            '--s3_bucket': bucket_name,
            '--s3_source_file_path': file_key,
            '--s3_dest_file_path': file_key_dest
        })