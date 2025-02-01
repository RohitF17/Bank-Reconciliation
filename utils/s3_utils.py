import boto3
from datetime import datetime
from botocore.exceptions import NoCredentialsError
import os

def download_latest_s3_files(bucket, prefix, tmp_dir):
    try:
        
        s3 = boto3.client('s3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'ap-south-1')
        )

        # List objects with prefix
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)['Contents']

        # Get latest date
        dates = [obj['Key'].split('=')[-1].split('/')[0] for obj in objects]
        latest_date = sorted(dates, key=lambda x: datetime.strptime(x, "%Y-%m-%d"))[-1]

        # Find latest file
        latest_prefix = f"{prefix}{latest_date}/"
        latest_obj = s3.list_objects_v2(Bucket=bucket, Prefix=latest_prefix)['Contents'][0]

        # Download file
        local_path = os.path.join(tmp_dir, f"{latest_date}.csv")
        s3.download_file(bucket, latest_obj['Key'], local_path)
    
    except NoCredentialsError:
        raise Exception("AWS credentials not found!")
