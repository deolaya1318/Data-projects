import boto3
import os

def lambda_handler(event, context):
    # Initialize S3 client
    s3 = boto3.client('s3')

    # Source and destination bucket names
    source_bucket = os.environ['SOURCE_BUCKET_NAME']
    destination_bucket = os.environ['DESTINATION_BUCKET_NAME']

    # File key to copy (assumes the event contains the key)
    file_key = 'Fintech-DBs/final_query_RDS_postgreSQL.csv'

    try:
        # Copy the file from source to destination bucket
        copy_source = {'Bucket': source_bucket, 'Key': file_key}
        s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=file_key)

        print(f"File {file_key} copied from {source_bucket} to {destination_bucket}")
        return {
            'statusCode': 200,
            'body': f"File {file_key} successfully copied."
        }

    except Exception as e:
        print(f"Error copying file: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error copying file: {str(e)}"
        }