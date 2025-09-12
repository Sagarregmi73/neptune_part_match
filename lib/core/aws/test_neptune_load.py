import os
import json
import boto3
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

# --------------------------
# CONFIG
# --------------------------
NEPTUNE_ENDPOINT = "db-neptune-1.cluster-clme8s022dqt.eu-north-1.neptune.amazonaws.com"
NEPTUNE_PORT = "8182"
AWS_REGION = "eu-north-1"
S3_FILE = "s3://s3-neptune2873/uploads/partmatch_edges.csv"
IAM_ROLE_ARN = "arn:aws:iam::730335617325:role/NeptuneFullS3Access"

# --------------------------
# AWS SESSION & CREDENTIALS
# --------------------------
session = boto3.session.Session()
credentials = session.get_credentials().get_frozen_credentials()

# --------------------------
# Build Neptune loader request
# --------------------------
payload = {
    "source": S3_FILE,
    "format": "csv",
    "iamRoleArn": IAM_ROLE_ARN,
    "region": AWS_REGION,
    "failOnError": True
}

loader_url = f"https://{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}/loader"

aws_request = AWSRequest(method="POST", url=loader_url, data=json.dumps(payload))
SigV4Auth(credentials, "neptune-db", AWS_REGION).add_auth(aws_request)

prepared_request = requests.Request(
    method=aws_request.method,
    url=aws_request.url,
    headers=dict(aws_request.headers),
    data=aws_request.body
).prepare()

# --------------------------
# Send request
# --------------------------
with requests.Session() as s:
    response = s.send(prepared_request, verify=True)
    print("HTTP Status:", response.status_code)
    try:
        print("Response JSON:", response.json())
    except:
        print("Response Text:", response.text)
