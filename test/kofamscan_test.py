import io
import traceback

import pytest
from pyspark.sql.types import Row

from cdmeventimporters.kofamscan import (
    CTS_JOB_ID,
    KOFAMSCAN_DB_SCHEMA,
    run_import,
)
from utils.misc import (
    assert_pyspark_rows_almost_equal,
    get_CTS_output_bucket,
    get_s3_client,
    set_up_basic_logging,
)
from utils.spark import SparkProvider, spark_session


# Header + separator that real kofamscan output emits.
_HEADER = '#\tgene name\tKO\tthrshld\tscore\tE-value\t"KO definition"'
_SEPARATOR = '#\t---------\t------\t-------\t------\t---------\t-------------'


def _write_kofamscan_tsv(s3cli, s3_path, rows):
    """Write a kofamscan annotations.tsv to S3.

    Each row is a tuple of (significant_marker, gene_name, KO, threshold, score, evalue,
    KO_definition). `significant_marker` is "*" for significant hits, "" otherwise.
    """
    bucket, key = s3_path.split("/", 1)
    buf = io.StringIO()
    buf.write(_HEADER + "\n")
    buf.write(_SEPARATOR + "\n")
    for r in rows:
        sig, gene, ko, thr, sc, ev, defn = r
        # Quote the definition as kofamscan does (matches the real output format).
        buf.write(f"{sig}\t{gene}\t{ko}\t{thr}\t{sc}\t{ev}\t\"{defn}\"\n")
    s3cli.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8"))


def _drop_namespace(spark, namespace: str):
    """See checkm2_test._drop_namespace for the Polaris/Iceberg rationale."""
    for tbl in spark.sql(f"SHOW TABLES IN {namespace}").collect():
        spark.sql(f"DROP TABLE IF EXISTS {namespace}.{tbl['tableName']} PURGE")
    spark.sql(f"DROP NAMESPACE IF EXISTS {namespace}")


# Pre-existing rows in the target table (older job)
_DB_INIT_DATA = [
    ("AE017199.1_99", "K00001", "old_job", 100.0, 250.0, 1e-50, "alcohol dehydrogenase"),
]

# File 1: two significant hits + one non-significant (which should be dropped by the
# importer's filter)
_FILE1_DATA = [
    ("*", "AE017199.1_1", "K19091",  38.33, 106.1, 1.2e-31, "CRISPR-associated endoribonuclease Cas6"),
    ("*", "AE017199.1_2", "K03724", 281.63, 744.2, 1.2e-224, "ATP-dependent helicase Lhr"),
    ("",  "AE017199.1_2", "K06877", 271.90, 209.9, 1.9e-63,  "DEAD/DEAH box helicase domain"),
]

# File 2: one significant hit
_FILE2_DATA = [
    ("*", "AE017199.1_5", "K02338", 200.0, 500.0, 1e-100, "DNA polymerase III subunit beta"),
]

_EXPECTED_FILE1 = [
    ("AE017199.1_1", "K19091", "tstjob1",  38.33, 106.1, 1.2e-31, "CRISPR-associated endoribonuclease Cas6"),
    ("AE017199.1_2", "K03724", "tstjob1", 281.63, 744.2, 1.2e-224, "ATP-dependent helicase Lhr"),
]

_EXPECTED_FULL = [
    ("AE017199.1_1",  "K19091", "tstjob2",  38.33, 106.1, 1.2e-31,  "CRISPR-associated endoribonuclease Cas6"),
    ("AE017199.1_2",  "K03724", "tstjob2", 281.63, 744.2, 1.2e-224, "ATP-dependent helicase Lhr"),
    ("AE017199.1_5",  "K02338", "tstjob2", 200.0,  500.0, 1e-100,   "DNA polymerase III subunit beta"),
    ("AE017199.1_99", "K00001", "old_job", 100.0,  250.0, 1e-50,    "alcohol dehydrogenase"),
]


set_up_basic_logging(force=False)


def _rows(data):
    fields = [f.name for f in KOFAMSCAN_DB_SCHEMA]
    # Field order in schema: gene_name, KO, cts_job_id, threshold, score, evalue, KO_definition
    return [Row(**dict(zip(fields, r))) for r in data]


@pytest.fixture(scope="module")
def minio_files():
    bucket = get_CTS_output_bucket()
    file1 = f"{bucket}/kofamscan/sub1/annotations.tsv"
    file2 = f"{bucket}/kofamscan/sub2/annotations.tsv"
    s3cli = get_s3_client()
    _write_kofamscan_tsv(s3cli, file1, _FILE1_DATA)
    _write_kofamscan_tsv(s3cli, file2, _FILE2_DATA)
    return file1, file2


def test_kofamscan_success_1_file_new_table(minio_files):
    user = "someuser"
    namespace = "kofamscan_test"
    table = f"{namespace}.kofamscan_single"
    file1 = minio_files[0]
    job_info = {
        "id": "tstjob1",
        "namespace_prefix": "",
        "outputs": [{"file": file1, "crc64nvme": "fake"}],
    }
    sparkprov = None
    try:
        sparkprov = SparkProvider("test_kofamscan", user)
        run_import(sparkprov, job_info, {"table": table})

        spark = sparkprov.spark
        res_df = spark.sql(f"SELECT * FROM {table}")
        actual = res_df.orderBy("gene_name", "KO").collect()
        assert_pyspark_rows_almost_equal(actual, _rows(_EXPECTED_FILE1))
    except Exception:
        traceback.print_exc()
        raise
    finally:
        if sparkprov:
            sparkprov.stop()
        spark_clean = spark_session("test_kofamscan_helper", user)
        _drop_namespace(spark_clean, namespace)
        spark_clean.stop()


def test_kofamscan_success_2_files_existing_table(minio_files):
    user = "testuser"
    namespace = "kofamscan_test"
    table = f"{namespace}.kofamscan_hits"
    file1, file2 = minio_files
    job_info = {
        "id": "tstjob2",
        "namespace_prefix": "",
        "outputs": [
            {"file": file1, "crc64nvme": "fake"},
            {"file": f"{get_CTS_output_bucket()}/kofamscan/sub1/other.tsv", "crc64nvme": "fake"},
            {"file": file2, "crc64nvme": "fake"},
        ],
    }
    spark_setup = spark_session("test_kofamscan_startup", user)
    sparkprov = None
    try:
        spark_setup.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        df = spark_setup.createDataFrame(_DB_INIT_DATA, schema=KOFAMSCAN_DB_SCHEMA)
        df.writeTo(table).using("iceberg").create()
        spark_setup.stop()

        sparkprov = SparkProvider("test_kofamscan", user)
        run_import(sparkprov, job_info, {"table": table})

        spark = sparkprov.spark
        res_df = spark.sql(f"SELECT * FROM {table}")
        actual = res_df.orderBy("gene_name", "KO", CTS_JOB_ID).collect()
        assert_pyspark_rows_almost_equal(actual, _rows(_EXPECTED_FULL))
    except Exception:
        traceback.print_exc()
        raise
    finally:
        spark_setup.stop()
        if sparkprov:
            sparkprov.stop()
        spark_clean = spark_session("test_kofamscan_helper", user)
        _drop_namespace(spark_clean, namespace)
        spark_clean.stop()


def test_kofamscan_no_annotation_files_raises(minio_files):
    user = "someuser"
    job_info = {
        "id": "tstjob3",
        "namespace_prefix": "",
        "outputs": [{"file": f"{get_CTS_output_bucket()}/kofamscan/sub1/other.tsv", "crc64nvme": "fake"}],
    }
    sparkprov = SparkProvider("test_kofamscan_err", user)
    try:
        with pytest.raises(ValueError, match="No KofamScan annotations.tsv files found"):
            run_import(sparkprov, job_info, {"table": "kofamscan_test.kofamscan_err"})
    finally:
        sparkprov.stop()


def test_kofamscan_no_table_in_metadata_raises(minio_files):
    user = "someuser"
    file1 = minio_files[0]
    job_info = {
        "id": "tstjob4",
        "namespace_prefix": "",
        "outputs": [{"file": file1, "crc64nvme": "fake"}],
    }
    sparkprov = SparkProvider("test_kofamscan_meta_err", user)
    try:
        with pytest.raises(ValueError, match="'table' key"):
            run_import(sparkprov, job_info, {})
    finally:
        sparkprov.stop()
