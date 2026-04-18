import pytest
import traceback
from pyspark.sql.types import Row

from cdmeventimporters.mmseqs2 import (
    run_import,
    MMSEQS2_CLUSTER_SCHEMA,
    CTS_JOB_ID,
)
from utils.misc import (
    assert_pyspark_rows_almost_equal,
    get_CTS_output_bucket,
    get_s3_client,
    set_up_basic_logging,
    write_tsv_to_s3,
)
from utils.spark import spark_session, SparkProvider


_CLUSTER_HEADERS = ["representative", "member"]

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
    write_tsv_to_s3(s3cli, file1, _CLUSTER_HEADERS, _FILE1_DATA)
    write_tsv_to_s3(s3cli, file2, _CLUSTER_HEADERS, _FILE2_DATA)
    return file1, file2


def test_mmseqs2_success_1_file_new_table(minio_files):
    user = "someuser"
    namespace_prefix = f"u_{user}__"
    file1 = minio_files[0]
    job_info = {
        "id": "tstjob1",
        "namespace_prefix": namespace_prefix,
        "outputs": [{"file": file1, "crc64nvme": "fake"}],
    }
    sparkprov = None
    try:
        sparkprov = SparkProvider("test_mmseqs2", user)
        run_import(sparkprov, job_info, {"deltatable": "mmseqs2_test.mmseqs2_single"})

        spark = sparkprov.spark
        res_df = spark.sql(f"SELECT * FROM {namespace_prefix}mmseqs2_test.mmseqs2_single")
        actual = res_df.orderBy("representative", "member").collect()
        assert_pyspark_rows_almost_equal(actual, _rows(_EXPECTED_FILE1))
    except Exception:
        traceback.print_exc()
        raise
    finally:
        if sparkprov:
            sparkprov.stop()
        spark_clean = spark_session("test_mmseqs2_helper", user)
        spark_clean.sql(f"DROP DATABASE IF EXISTS {namespace_prefix}mmseqs2_test CASCADE")
        spark_clean.stop()


def test_mmseqs2_success_2_files_existing_table(minio_files):
    user = "testuser"
    namespace_prefix = f"u_{user}__"
    file1, file2 = minio_files
    job_info = {
        "id": "tstjob2",
        "namespace_prefix": namespace_prefix,
        "outputs": [
            {"file": file1, "crc64nvme": "fake"},
            {"file": f"{get_CTS_output_bucket()}/mmseqs2/sub1/other_file.tsv", "crc64nvme": "fake"},
            {"file": file2, "crc64nvme": "fake"},
        ],
    }
    spark_setup = spark_session("test_mmseqs2_startup", user)
    sparkprov = None
    try:
        spark_setup.sql(f"CREATE DATABASE IF NOT EXISTS {namespace_prefix}mmseqs2_test")
        df = spark_setup.createDataFrame(_DB_INIT_DATA, schema=MMSEQS2_CLUSTER_SCHEMA)
        df.write.mode("overwrite").option("compression", "snappy").format("delta").saveAsTable(
            f"{namespace_prefix}mmseqs2_test.mmseqs2_clusters"
        )
        spark_setup.stop()

        sparkprov = SparkProvider("test_mmseqs2", user)
        run_import(sparkprov, job_info, {"deltatable": "mmseqs2_test.mmseqs2_clusters"})

        spark = sparkprov.spark
        res_df = spark.sql(f"SELECT * FROM {namespace_prefix}mmseqs2_test.mmseqs2_clusters")
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
        spark_clean.sql(f"DROP DATABASE IF EXISTS {namespace_prefix}mmseqs2_test CASCADE")
        spark_clean.stop()


def test_mmseqs2_no_cluster_files_raises(minio_files):
    user = "someuser"
    namespace_prefix = f"u_{user}__"
    job_info = {
        "id": "tstjob3",
        "namespace_prefix": namespace_prefix,
        "outputs": [{"file": f"{get_CTS_output_bucket()}/mmseqs2/sub1/rep_seq.fasta", "crc64nvme": "fake"}],
    }
    sparkprov = SparkProvider("test_mmseqs2_err", user)
    try:
        with pytest.raises(ValueError, match="No MMseqs2 cluster TSV files found"):
            run_import(sparkprov, job_info, {"deltatable": "mmseqs2_test.mmseqs2_err"})
    finally:
        sparkprov.stop()


def test_mmseqs2_no_deltatable_in_metadata_raises(minio_files):
    user = "someuser"
    namespace_prefix = f"u_{user}__"
    file1 = minio_files[0]
    job_info = {
        "id": "tstjob4",
        "namespace_prefix": namespace_prefix,
        "outputs": [{"file": file1, "crc64nvme": "fake"}],
    }
    sparkprov = SparkProvider("test_mmseqs2_meta_err", user)
    try:
        with pytest.raises(ValueError, match="deltatable"):
            run_import(sparkprov, job_info, {})
    finally:
        sparkprov.stop()
