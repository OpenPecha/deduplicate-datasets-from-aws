import boto3

# Define S3 Bucket Name
BUCKET_NAME = "monlam.ai.stt"
# Initialize S3 client
s3_client = boto3.client("s3")
