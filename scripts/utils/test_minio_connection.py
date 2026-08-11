from dotenv import load_dotenv
import os
import boto3
from botocore.client import Config

load_dotenv()

endpoint = os.getenv("MINIO_ENDPOINT")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")
bucket_name = os.getenv("MINIO_BUCKET")

s3_client = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

response = s3_client.list_buckets()

print("Connected to MinIO successfully.")
print("Available buckets:")

for bucket in response["Buckets"]:
    print("-", bucket["Name"])

print()
print(f"Checking bucket: {bucket_name}")

objects = s3_client.list_objects_v2(
    Bucket=bucket_name,
    MaxKeys=20
)

if "Contents" in objects:
    print("Objects/prefixes found:")
    for obj in objects["Contents"]:
        print("-", obj["Key"])
else:
    print("Bucket exists, but no objects found yet.")