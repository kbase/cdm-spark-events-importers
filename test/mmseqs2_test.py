import csv
import io
import traceback

import pytest
from pyspark.sql.types import Row

from cdmeventimporters.mmseqs2 import (
    CTS_JOB_ID,
    MMSEQS2_CLUSTER_SCHEMA,
    run_import,
)
from utils.misc import (
    assert_pyspark_rows_almost_equal,
    get_CTS_output_bucket,
    get_s3_client,
    set_up_basic_logging,
)
from utils.spark import SparkProvider, spark_session


def _write_tsv_no_header(s3cli, s3_path, rows):
    """Write TSV rows to S3 without a header - matches actual MMseqs2 output format."""
    bucket, key = s3_path.split("/", 1)
    buf = io.StringIO()
    csv.writer(buf, delimiter="\t", lineterminator="\n").writerows(rows)
    buf.seek(0)
    s3cli.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8"))


def _drop_namespace(spark, namespace: str):
    """Drop every table in the namespace, then the namespace itself.

    Same pattern as checkm2_test: Polaris rejects `DROP NAMESPACE ... CASCADE`, so the
    tables have to come out one at a time before the namespace. `PURGE` removes data
    + metadata files from S3 so warehouse orphans don't accumulate across test runs.
    """
    for tbl in spark.sql(f"SHOW TABLES IN {namespace}").collect():
        spark.sql(f"DROP TABLE IF EXISTS {namespace}.{tbl['tableName']} PURGE")
    spark.sql(f"DROP NAMESPACE IF EXISTS {namespace}")


# Initial table state (pre-existing rows from an older job)
_DB_INIT_DATA = [
    ("seq_keep", "seq_keep", "old_job"),
]

# File 1: two cluster memberships
_FILE1_DATA = [
    ("seq_A", "seq_A"),
    ("seq_A", "seq_B"),
]

# File 2: one cluster membership
_FILE2_DATA = [
    ("seq_C", "seq_C"),
]

# Expected after importing file1 into a fresh table
_EXPECTED_FILE1 = [
    ("seq_A", "seq_A", "tstjob1"),
    ("seq_A", "seq_B", "tstjob1"),
]

# Expected after importing both files into a table that already has _DB_INIT_DATA
_EXPECTED_FULL = [
    ("seq_A", "seq_A", "tstjob2"),
    ("seq_A", "seq_B", "tstjob2"),
    ("seq_C", "seq_C", "tstjob2"),
    ("seq_keep", "seq_keep", "old_job"),
]


set_up_basic_logging(force=False)


def _rows(data):
    fields = [f.name for f in MMSEQS2_CLUSTER_SCHEMA]
    return [Row(**dict(zip(fields, r))) for r in data]


@pytest.fixture(scope="module")
def minio_files():
    bucket = get_CTS_output_bucket()
    file1 = f"{bucket}/mmseqs2/sub1/cluster_results_cluster.tsv"
    file2 = f"{bucket}/mmseqs2/sub2/cluster_results_cluster.tsv"
    s3cli = get_s3_client()
    # MMseqs2 cluster TSV has no header - write raw rows only
    _write_tsv_no_header(s3cli, file1, _FILE1_DATA)
    _write_tsv_no_header(s3cli, file2, _FILE2_DATA)
    return file1, file2


def test_mmseqs2_success_1_file_new_table(minio_files):
    user = "someuser"
    namespace = "mmseqs2_test"
    table = f"{namespace}.mmseqs2_single"
    file1 = minio_files[0]
    job_info = {
        "id": "tstjob1",
        # Empty under Polaris/Iceberg; preserved as the back-compat contract.
        "namespace_prefix": "",
        "outputs": [{"file": file1, "crc64nvme": "fake"}],
    }
    sparkprov = None
    try:
        sparkprov = SparkProvider("test_mmseqs2", user)
        run_import(sparkprov, job_info, {"table": table})

        spark = sparkprov.spark
        res_df = spark.sql(f"SELECT * FROM {table}")
        actual = res_df.orderBy("representative", "member").collect()
        assert_pyspark_rows_almost_equal(actual, _rows(_EXPECTED_FILE1))
    except Exception:
        traceback.print_exc()  # can get shadowed by exception in the finally block
        raise
    finally:
        if sparkprov:
            sparkprov.stop()
        spark_clean = spark_session("test_mmseqs2_helper", user)
        _drop_namespace(spark_clean, namespace)
        spark_clean.stop()


def test_mmseqs2_success_2_files_existing_table(minio_files):
    user = "testuser"
    namespace = "mmseqs2_test"
    table = f"{namespace}.mmseqs2_clusters"
    file1, file2 = minio_files
    job_info = {
        "id": "tstjob2",
        "namespace_prefix": "",
        "outputs": [
            {"file": file1, "crc64nvme": "fake"},
            {"file": f"{get_CTS_output_bucket()}/mmseqs2/sub1/other_file.tsv", "crc64nvme": "fake"},
            {"file": file2, "crc64nvme": "fake"},
        ],
    }
    spark_setup = spark_session("test_mmseqs2_startup", user)
    sparkprov = None
    try:
        spark_setup.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        df = spark_setup.createDataFrame(_DB_INIT_DATA, schema=MMSEQS2_CLUSTER_SCHEMA)
        df.writeTo(table).using("iceberg").create()
        spark_setup.stop()

        sparkprov = SparkProvider("test_mmseqs2", user)
        run_import(sparkprov, job_info, {"table": table})

        spark = sparkprov.spark
        res_df = spark.sql(f"SELECT * FROM {table}")
        actual = res_df.orderBy("representative", "member", CTS_JOB_ID).collect()
        assert_pyspark_rows_almost_equal(actual, _rows(_EXPECTED_FULL))
    except Exception:
        traceback.print_exc()
        raise
    finally:
        spark_setup.stop()
        if sparkprov:
            sparkprov.stop()
        spark_clean = spark_session("test_mmseqs2_helper", user)
        _drop_namespace(spark_clean, namespace)
        spark_clean.stop()


def test_mmseqs2_no_cluster_files_raises(minio_files):
    user = "someuser"
    job_info = {
        "id": "tstjob3",
        "namespace_prefix": "",
        "outputs": [{"file": f"{get_CTS_output_bucket()}/mmseqs2/sub1/rep_seq.fasta", "crc64nvme": "fake"}],
    }
    sparkprov = SparkProvider("test_mmseqs2_err", user)
    try:
        with pytest.raises(ValueError, match="No MMseqs2 cluster TSV files found"):
            run_import(sparkprov, job_info, {"table": "mmseqs2_test.mmseqs2_err"})
    finally:
        sparkprov.stop()


def test_mmseqs2_no_table_in_metadata_raises(minio_files):
    user = "someuser"
    file1 = minio_files[0]
    job_info = {
        "id": "tstjob4",
        "namespace_prefix": "",
        "outputs": [{"file": file1, "crc64nvme": "fake"}],
    }
    sparkprov = SparkProvider("test_mmseqs2_meta_err", user)
    try:
        with pytest.raises(ValueError, match="'table' key"):
            run_import(sparkprov, job_info, {})
    finally:
        sparkprov.stop()
