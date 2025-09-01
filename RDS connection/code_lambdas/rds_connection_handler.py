import boto3
import os
import psycopg2
import csv
import json
import tempfile

def lambda_handler(event, context):
    # Environment variables
    source_s3_bucket = os.environ['SOURCE_S3_BUCKET']  # Bucket where the SQL script is initially stored
    destination_s3_bucket = os.environ['DESTINATION_S3_BUCKET']  # Bucket for copying the script and saving results
    sql_script_key = f"code/{os.environ['SQL_SCRIPT_KEY']}"
    rds_host = os.environ['RDS_HOST']
    rds_user = os.environ['RDS_USER']
    rds_password = os.environ['RDS_PASSWORD']
    rds_dbname = os.environ['RDS_DBNAME']

    s3 = boto3.client('s3')

    # Step 1: Download SQL script from source S3 bucket
    try:
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            s3.download_fileobj(source_s3_bucket, sql_script_key, tmp_file)
            tmp_file_path = tmp_file.name
        print(f"Downloaded SQL script {sql_script_key} from bucket {source_s3_bucket}")
    except Exception as e:
        print(f"Error downloading SQL script: {str(e)}")
        return {'statusCode': 500, 'body': f"Error downloading SQL script: {str(e)}"}

    # Step 2: Copy SQL script to destination S3 bucket
    try:
        s3.upload_file(tmp_file_path, destination_s3_bucket, sql_script_key)
        print(f"Copied SQL script {sql_script_key} to bucket {destination_s3_bucket}")
    except Exception as e:
        print(f"Error copying SQL script to destination bucket: {str(e)}")
        return {'statusCode': 500, 'body': f"Error copying SQL script to destination bucket: {str(e)}"}

    # Step 3: Connect to RDS PostgreSQL
    try:
        conn = psycopg2.connect(
            host=rds_host,
            user=rds_user,
            password=rds_password,
            dbname=rds_dbname,
            connect_timeout=10
        )
        print("Connected to RDS PostgreSQL instance.")
    except Exception as e:
        print(f"Error connecting to RDS: {str(e)}")
        return {'statusCode': 500, 'body': f"Error connecting to RDS: {str(e)}"}

    # Step 4: Execute SQL script line by line
    result = None
    try:
        with open(tmp_file_path, 'r') as sql_file:
            sql_lines = sql_file.read().split(';')
        with conn.cursor() as cur:
            for idx, statement in enumerate(sql_lines):
                statement = statement.strip()
                if not statement:
                    continue
                print(f"Executing SQL statement {idx+1}...")
                cur.execute(statement)
                if cur.description:  # If the statement returns rows
                    result = cur.fetchall()
        conn.commit()
        print("SQL script executed successfully.")
    except Exception as e:
        print(f"Error executing SQL script: {str(e)}")
        return {'statusCode': 500, 'body': f"Error executing SQL script: {str(e)}"}
    finally:
        conn.close()

    # Step 5: Save result to destination S3 bucket as CSV and JSON
    output_key_csv = sql_script_key.replace('.sql', '_result.csv')
    output_key_json = sql_script_key.replace('.sql', '_result.json')
    try:
        if result:
            # Save as CSV
            with tempfile.NamedTemporaryFile(mode='w+', delete=False, newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(result)
                csvfile_path = csvfile.name
            s3.upload_file(csvfile_path, destination_s3_bucket, output_key_csv)
            print(f"Result saved as CSV to {output_key_csv} in bucket {destination_s3_bucket}")
            # Save as JSON
            with tempfile.NamedTemporaryFile(mode='w+', delete=False) as jsonfile:
                json.dump(result, jsonfile)
                jsonfile_path = jsonfile.name
            s3.upload_file(jsonfile_path, destination_s3_bucket, output_key_json)
            print(f"Result saved as JSON to {output_key_json} in bucket {destination_s3_bucket}")
        else:
            print("No result to save.")
    except Exception as e:
        print(f"Error saving result to S3: {str(e)}")
        return {'statusCode': 500, 'body': f"Error saving result to S3: {str(e)}"}

    return {
        'statusCode': 200,
        'body': f"SQL script executed. Results saved to {output_key_csv} and {output_key_json} in bucket {destination_s3_bucket}."
    }