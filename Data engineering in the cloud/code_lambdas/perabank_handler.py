import boto3
import os

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    source_bucket = os.environ['SOURCE_BUCKET_NAME']
    destination_bucket = os.environ['DESTINATION_BUCKET_NAME']
    source_key = 'Fintech-DBs/comportamiento_digital.json'

    try:
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=source_key)
        return {
            'statusCode': 200,
            'body': f"File {source_key} copied from {source_bucket} to {destination_bucket}"
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }