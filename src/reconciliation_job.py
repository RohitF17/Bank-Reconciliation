from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, abs, when
# from  utils.s3_utils import download_latest_s3_files
import yaml
import os
from dotenv import load_dotenv
import boto3
from datetime import datetime
from botocore.exceptions import NoCredentialsError
import os

load_dotenv() 
def download_latest_s3_files(bucket, prefix, tmp_dir, date_input='01-31-2025.csv'):
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'ap-south-1')
        )

        # Check for available files
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response:
            raise Exception(f"No files found in bucket '{bucket}' with prefix '{prefix}'")

        # Get dataset type from prefix (e.g., "bank_statements" from "bank_statements/date_init=")
        dataset_type = prefix.split('/')[0]
        
        # Create unique filename
        unique_filename = f"{dataset_type}_{date_input}"
        local_path = os.path.join(tmp_dir, unique_filename)

        # Find matching file
        target_file = f"{prefix}{date_input}"
        file_found = False

        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith(date_input):
                file_found = True
                s3.download_file(bucket, key, local_path)
                print(f"Downloaded {key} to {local_path}")
                return local_path

        if not file_found:
            raise Exception(f"No file found for date '{date_input}' in prefix '{prefix}'")

    except Exception as e:
        print(f"Error downloading file: {str(e)}")
def load_config():
    with open("config/reconciliation_config.yaml") as f:
        return yaml.safe_load(f)

def read_csv(spark, path):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path)

def reconcile_transactions(bank_df: DataFrame, db_df: DataFrame, config: dict) -> DataFrame:
    # Extract configuration parameters
    common_id = config['app']['common_id']
    amount_tolerance = config['app']['amount_tolerance']
    db_success_status = config['app']['status_mapping']['db_success']
    bank_success_status = config['app']['status_mapping']['bank_success']

    # Rename columns for easier access and consistent joins
    bank_df = bank_df.withColumnRenamed("Reference ID", "transaction_id") \
                     .withColumnRenamed("Amount", "bank_amount") \
                     .withColumnRenamed("Date", "transaction_date")
                     
    db_df = db_df.withColumnRenamed("Reference ID", "transaction_id") \
                 .withColumnRenamed("Amount", "db_amount")

    # Perform an outer join on the specified common ID
    reconciled_df = bank_df.alias("bank").join(
        db_df.alias("db"),
        on="transaction_id",
        how="outer"
    )

    # Define matching conditions
    status_cond = col("db.Transaction Type").isin(db_success_status) & \
                  col("bank.Transaction Type").isin(bank_success_status)

    amount_cond = abs(col("bank.bank_amount") - col("db.db_amount")) <= amount_tolerance

    # Define tagging logic for reconciliation status
    reconciled_df = reconciled_df.withColumn(
        "reconciliation_status",
        when(col("bank.transaction_id").isNull(), "Missing in Bank Statement")
        .when(col("db.transaction_id").isNull(), "Missing in DB Statement")
        .when(status_cond & amount_cond, "Reconciled")
        .otherwise("Discrepancy Found")
    )

    result_df = reconciled_df.select(
        col("transaction_id"),
        col("bank.bank_amount").alias("bank_amount"),
        col("db.db_amount").alias("db_amount"),
        col("bank.transaction_date").alias("transaction_date"),
        col("reconciliation_status")
    )

    return result_df

def main():
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("BankReconciliation") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()
    
    # Load config
    config = load_config()
    
    # Download latest files
    s3_config = config['s3']
    bank_path = download_latest_s3_files(
        s3_config['bucket'],
        s3_config['bank_prefix'],
        config['app']['tmp_dir']
    )
    db_path = download_latest_s3_files(
        s3_config['bucket'],
        s3_config['db_prefix'],
        config['app']['tmp_dir']
    )
    
    # Read data
    bank_df = read_csv(spark, bank_path)
    db_df = read_csv(spark, db_path)
    
    # Perform reconciliation
    result_df = reconcile_transactions(bank_df, db_df, config)
    
    result_df.show()
    # Generate reports
    output_path = f"s3a://{s3_config['bucket']}/{s3_config['output_path']}"
    
    result_df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{output_path}/full_report")
        
    # Save tagged and untagged separately
    result_df.filter(col("reconciliation_status") == "Reconciled") \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{output_path}/tagged_transactions")
        
    result_df.filter(col("reconciliation_status") != "Reconciled") \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{output_path}/untagged_transactions")
        
    # Cleanup
    spark.stop()

if __name__ == "__main__":
    main()