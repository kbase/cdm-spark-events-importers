import boto3
from botocore.client import BaseClient
import csv
import io
import logging
import math
import os
from pyspark.sql.types import Row


def set_up_basic_logging(force=False):
    logging.basicConfig(level=logging.INFO, force=force)


def get_s3_client() -> BaseClient:  # apparently boto dynamically generates the client
    return boto3.client(
        "s3",
        endpoint_url=os.environ["IMP_MINIO_URL"],
        aws_access_key_id=os.environ["IMP_MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["IMP_MINIO_SECRET_KEY"],
    )


def get_CTS_output_bucket() -> str:
    return os.environ["IMP_MINIO_IMPORTER_RAW_BUCKET"]


def write_tsv_to_s3(
    s3cli: BaseClient,
    s3_path: str,
    headers: list[str],
    rows: list[list[str | int | float | None]]
):
    """
    Write a TSV file to the specified S3 location.

    s3cli - initialized boto3 S3 client.
    s3_path - full S3 path starting with the bucket, e.g., 'my-bucket/path/to/file.tsv'.
    headers - list of column headers.
    rows - data rows, where each inner list represents one row.

    raises ValueError If the S3 path is not valid.
    """
    # Parse bucket and key
    parts = s3_path.split('/', 1)
    if len(parts) != 2:
        raise ValueError("s3_path must be in the format 'bucket/key'")
    bucket, key = parts

    # Write TSV content to an in-memory buffer
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter='\t', lineterminator='\n')
    writer.writerow(headers)
    writer.writerows(rows)

    # Reset buffer position
    buffer.seek(0)

    # Upload to S3
    s3cli.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode('utf-8'),
        ContentType='text/tab-separated-values'
    )


def assert_pyspark_rows_almost_equal(
    received_rows: list[Row],
    expected_rows: list[Row],
    rel_tol: float = 1e-6,
    abs_tol: float = 1e-12,
):
    assert len(received_rows) == len(expected_rows), (
        f"Row count mismatch: received {len(received_rows)}, expected {len(expected_rows)}"
    )

    for i, (received, expected) in enumerate(zip(received_rows, expected_rows)):
        received_dict = received.asDict()
        expected_dict = expected.asDict()
        assert received_dict.keys() == expected_dict.keys(), (
            f"Field mismatch at index {i}:\n"
            f"Received fields: {received_dict.keys()}\n"
            f"Expected fields: {expected_dict.keys()}"
        )

        for field in received_dict:
            val1 = received_dict[field]
            val2 = expected_dict[field]

            if isinstance(val1, float) and isinstance(val2, float):
                if not math.isclose(val1, val2, rel_tol=rel_tol, abs_tol=abs_tol):
                    raise AssertionError(
                        f"Float mismatch at index {i}, field '{field}':\n"
                        + f"Received: {val1} (type {type(val1)})\n"
                        + f"Expected: {val2} (type {type(val2)})\n"
                        f"Relative tolerance: {rel_tol}, absolute tolerance: {abs_tol}"
                    )
            else:
                if val1 != val2:
                    raise AssertionError(
                        f"Value mismatch at index {i}, field '{field}':\n"
                        f"Received: {val1} (type {type(val1)})\n"
                        f"Expected: {val2} (type {type(val2)})"
                    )
