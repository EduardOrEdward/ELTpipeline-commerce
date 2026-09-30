"""Bootstrap the CI S3 bucket with administrative credentials."""

import os

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
BUCKET = os.getenv("S3_BUCKET", "elt-raw")


def main() -> None:
    access_key = os.environ["S3_ACCESS_KEY"]
    secret_key = os.environ["S3_SECRET_KEY"]

    client = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )

    try:
        client.head_bucket(Bucket=BUCKET)
        print(f"[OK] S3 bucket already exists: {BUCKET}")
        return
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status != 404:
            raise

    client.create_bucket(Bucket=BUCKET)
    print(f"[OK] S3 bucket created: {BUCKET}")


if __name__ == "__main__":
    main()
