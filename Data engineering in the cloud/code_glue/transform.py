import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from datetime import datetime
from pyspark.sql.utils import AnalysisException
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import ArrayType, StringType, StructField

# Parse command-line arguments
args = getResolvedOptions(sys.argv,
                            ['JOB_NAME',
                            'INPUT_BUCKET',
                            'OUTPUT_BUCKET'])

# Create a SparkContext and GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")

# Create a Glue Job instance
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# Function to read JSON from S3 using PySpark
def read_json_from_s3(spark, s3_path):
    try:
        # Read data from S3 and create a DataFrame
        df = spark.read.json(s3_path).cache()
        count = df.count()
        print(f"Number of records read from {s3_path}: {count}")
        # Check for corrupt records
        if '_corrupt_record' in df.columns:
            corrupt_count = df.filter(df["_corrupt_record"].isNotNull()).count()
            print(f"Corrupt records in {s3_path}: {corrupt_count}")
            df = df.filter(df["_corrupt_record"].isNull())
        return df
    except AnalysisException as e:
        print(f"Error reading from {s3_path}: {str(e)}")
        return None

# Function to read CSV from S3 using PySpark
def read_csv_from_s3(spark, s3_path):
    try:
        df = spark.read.option("header", True).csv(s3_path).cache()
        count = df.count()
        print(f"Number of records read from {s3_path}: {count}")
        # Check for corrupt records
        if '_corrupt_record' in df.columns:
            corrupt_count = df.filter(df["_corrupt_record"].isNotNull()).count()
            print(f"Corrupt records in {s3_path}: {corrupt_count}")
            df = df.filter(df["_corrupt_record"].isNull())
        return df
    except AnalysisException as e:
        print(f"Error reading from {s3_path}: {str(e)}")
        return None

# Function to validate the DataFrame and write to Parquet
def validate_and_write_parquet(df, database_name, table_name, output_s3_path, spark):

    # Check if the database exists using Spark SQL
    
    try:
        spark.sql(f"USE {database_name}")
    except:
        print(f"Database {database_name} does not exist. Creating the database.")
        spark.sql(f"CREATE DATABASE {database_name}")
        
        
    # Check if the table exists
    try:
        spark.table(table_name)
        print(f"Table {table_name} exists. Overwriting the table.")
        # If table exists, overwrite it
        df.write.option('path',output_s3_path).format("parquet").mode("overwrite").saveAsTable(f"{database_name}.{table_name}")
    except AnalysisException:
        print(f"Table {table_name} does not exist. Creating the table.")
        # If table doesn't exist, create the table
        
        df.write.option('path',output_s3_path).format("parquet").saveAsTable(f"{database_name}.{table_name}")


# Main logic
def main():
    # Generate timestamp to be used in S3 paths (not needed for static files)
    # timestamp = datetime.now().strftime('%Y-%m-%d')

    # Define S3 paths for input and output data based on handler source keys
    # contoso_bank_handler: SOURCE_BUCKET_PATH:
    input_s3_contoso = f"{args['INPUT_BUCKET']}accepted_2007_to_2018Q4.csv"
    output_s3_contoso = f"{args['OUTPUT_BUCKET']}Contoso/"

    # perabank_handler: SOURCE_BUCKET_PATH:
    input_s3_perabank = f"{args['INPUT_BUCKET']}comportamiento_digital.json"
    output_s3_perabank = f"{args['OUTPUT_BUCKET']}Perabank/"

    # colombia_bank_handler: SOURCE_BUCKET_PATH: Raw/Colombia/
    input_s3_colombia = f"{args['INPUT_BUCKET']}datos_demograficos.csv"
    output_s3_colombia = f"{args['OUTPUT_BUCKET']}Colombia/"
    
    # industrial_bank_handler: SOURCE_BUCKET_PATH: Raw/Industrial/
    input_s3_industrial = f"{args['INPUT_BUCKET']}final_query_RDS_postgreSQL.csv"
    output_s3_industrial = f"{args['OUTPUT_BUCKET']}IndustrialBank/"

    # Read CSV for Contoso
    df_contoso = read_csv_from_s3(spark, input_s3_contoso)
    # Read CSV for Colombia
    df_colombia = read_csv_from_s3(spark, input_s3_colombia)
    # Read JSON for Perabank
    df_perabank = read_json_from_s3(spark, input_s3_perabank)

    # Check if the perabank table exists before reading
    if spark.catalog.tableExists("database_perabank.table_perabank"):
        print("Table 'database_perabank.table_perabank' exists in Glue Catalog.")
    else:
        print("Table 'database_perabank.table_perabank' does NOT exist in Glue Catalog. It will be created on write.")
        # Create an empty DataFrame with the correct schema for perabank
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
        perabank_schema = StructType([
            StructField("cliente_id", StringType(), True),
            StructField("email", StringType(), True),
            StructField("dispositivos_activos", IntegerType(), True),
            StructField("frecuencia_login_mensual", IntegerType(), True),
            StructField("preferencia_canal", StringType(), True),
            StructField("ultima_actividad", StringType(), True),
            StructField("servicios_contratados", ArrayType(StringType()), True)
        ])
        df_perabank = spark.createDataFrame([], perabank_schema)
        print("Created empty perabank DataFrame with correct schema.")

    # Read CSV for Industrial Bank join result
    df_industrial = read_csv_from_s3(spark, input_s3_industrial)

    # Validate and write to Parquet
    validate_and_write_parquet(df_contoso, "database_contoso", "table_contoso", output_s3_contoso, spark)
    validate_and_write_parquet(df_colombia, "database_colombia", "table_colombia", output_s3_colombia, spark)
    validate_and_write_parquet(df_perabank, "database_perabank", "table_perabank", output_s3_perabank, spark)
    validate_and_write_parquet(df_industrial, "database_industrial", "table_industrial", output_s3_industrial, spark)

if __name__ == "__main__":
    main()
