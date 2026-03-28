"""
Set up Ceph RGW buckets and an importer IAM user for testing.

Creates the required S3 buckets, an IAM user with an inline policy granting access to
those buckets, and writes the generated credentials to a file so the test container can
load them at pytest startup.

Intended to run as a short-lived Docker Compose service that completes before the
test-container starts.
"""

import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


def _s3_client(url: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


def _iam_client(url: str, access_key: str, secret_key: str):
    return boto3.client(
        "iam",
        endpoint_url=url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


def _create_bucket(s3, bucket: str):
    try:
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket {bucket!r} created")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"Bucket {bucket!r} already exists")
        else:
            raise


def _create_iam_user(iam, username: str):
    try:
        iam.create_user(UserName=username)
        print(f"IAM user {username!r} created")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            print(f"IAM user {username!r} already exists")
        else:
            raise


def main():
    url = os.environ["CEPH_URL"]
    admin_key = os.environ["CEPH_ADMIN_KEY"]
    admin_secret = os.environ["CEPH_ADMIN_SECRET"]
    raw_bucket = os.environ["CEPH_RAW_BUCKET"]
    sql_bucket = os.environ["CEPH_SQL_BUCKET"]
    warehouse_path = os.environ["CEPH_WAREHOUSE_PATH"]
    importer_user = os.environ["CEPH_IMPORTER_USER"]
    creds_file = os.environ["CEPH_CREDS_FILE"]

    print(f"\n*** starting Ceph bucket & user setup against {url} ***\n")

    s3 = _s3_client(url, admin_key, admin_secret)
    iam = _iam_client(url, admin_key, admin_secret)

    # Create buckets
    _create_bucket(s3, raw_bucket)
    _create_bucket(s3, sql_bucket)

    # Placeholder so Hive metastore can stat the warehouse path on startup
    s3.put_object(Bucket=sql_bucket, Key=f"{warehouse_path}/.keep", Body=b"")
    print(f"Wrote warehouse placeholder to {sql_bucket}/{warehouse_path}/.keep")

    # Create IAM user
    _create_iam_user(iam, importer_user)

    # Generate access key — Ceph returns the key pair, we cannot pre-specify it
    resp = iam.create_access_key(UserName=importer_user)
    access_key = resp["AccessKey"]["AccessKeyId"]
    secret_key = resp["AccessKey"]["SecretAccessKey"]
    print(f"Created access key for {importer_user!r}: {access_key}")

    # Inline policy — Ceph does not support managed (attached) policies
    policy_path = Path(__file__).parent / "test-importers-read-write-policy.json"
    policy = policy_path.read_text()
    iam.put_user_policy(
        UserName=importer_user,
        PolicyName="test-importers-read-write-policy",
        PolicyDocument=policy,
    )
    print(f"Inline policy attached to {importer_user!r}")

    # Write credentials to the shared volume for conftest.py to load
    with open(creds_file, "w") as f:
        f.write(f"IMP_MINIO_ACCESS_KEY={access_key}\n")
        f.write(f"IMP_MINIO_SECRET_KEY={secret_key}\n")
    print(f"Wrote importer credentials to {creds_file}")

    print("\n*** Ceph setup complete ***\n")


if __name__ == "__main__":
    main()
