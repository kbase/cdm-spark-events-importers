# TODO CHECKM2 add more test cases, failing tests etc.

from pyspark.sql.types import Row
import pytest
import traceback
from typing import Any

from cdmeventimporters.checkm2 import (
    run_import,
    CHECKM2_DB_SCHEMA,
    CHECKM2_NAME_FIELD,
    CTS_JOB_ID
)
from utils.misc import (
    assert_pyspark_rows_almost_equal,
    get_CTS_output_bucket,
    get_s3_client,
    set_up_basic_logging,
    write_tsv_to_s3,
)
from utils.spark import spark_session, SparkProvider


_CHECKM2_FILE_HEADERS = [field.name for field in CHECKM2_DB_SCHEMA if field.name != CTS_JOB_ID]


_CHECKM2_DB_INIT_DATA =  [
    ("GCA_replace", "old_job", .87, .23, "model1", 5, .45, 146, .46, 42, .89, 32, 1, 12, None),
    ("GCA_keep", "old_job2", .88, .24, "model2", 6, .46, 147, .47, 43, .90, 33, 2, 13, "Lovely"),
]


_CHECKM2_FILE1_DATA =  [
    ("GCA_replace", .89, .25, "model3", 7, .47, 148, .48, 44, .91, 34, 3, 14, "hello"),
    ("GCA_ecoli", .90, .26, "model4", 8, .48, 149, .49, 45, .92, 35, 4, 15, None),
]


_CHECKM2_FILE2_DATA =  [
    ("GCA_bsubtilis", .91, .27, "mod", 9, .49, 150, .50, 46, .93, 36, 5, 16, "whoo"),
]


_CHECKM2_EXPECTED_DATA_FILE1 = [
    ("GCA_ecoli", "tstjob1", .90, .26, "model4", 8, .48, 149, .49, 45, .92, 35, 4, 15, None),
    ("GCA_replace", "tstjob1", .89, .25, "model3", 7, .47, 148, .48, 44, .91, 34, 3, 14, "hello"),
]


_CHECKM2_EXPECTED_DATA_FULL = [
    ("GCA_bsubtilis", "tstjob2", .91, .27, "mod", 9, .49, 150, .50, 46, .93, 36, 5, 16, "whoo"),
    ("GCA_ecoli", "tstjob2", .90, .26, "model4", 8, .48, 149, .49, 45, .92, 35, 4, 15, None),
    ("GCA_keep", "old_job2", .88, .24, "model2", 6, .46, 147, .47, 43, .90, 33, 2, 13, "Lovely"),
    ("GCA_replace", "tstjob2", .89, .25, "model3", 7, .47, 148, .48, 44, .91, 34, 3, 14, "hello"),
]


# Set force to True to see checkm2 logs. pytest -s or -o log_cli=true doesn't seem to work.
# Will cause a logging error to be thrown at the end of the tests
set_up_basic_logging(force=False)


def expected_data_to_rows(expected: list[tuple[Any, ...]]):
    ret = []
    for r in expected:
        ret.append(Row(**{f: d for f, d in zip([field.name for field in CHECKM2_DB_SCHEMA], r)}))
    return ret


@pytest.fixture(scope="module")
def minio_files():
    bucket = get_CTS_output_bucket()
    file1 = f"{bucket}/checkm2/sub1/quality_report.tsv"
    file2 = f"{bucket}/checkm2/sub2/quality_report.tsv"
    s3cli = get_s3_client()
    write_tsv_to_s3(s3cli, file1, _CHECKM2_FILE_HEADERS, _CHECKM2_FILE1_DATA)
    write_tsv_to_s3(s3cli, file2, _CHECKM2_FILE_HEADERS, _CHECKM2_FILE2_DATA)
    return file1, file2


def test_checkm2_success_1_file_new_table(minio_files):
    # This test is slooow. Probably better to make a fixture that sets everything up and 
    # reuse it for other tests.
    user = "someuser"
    namespace_prefix = f"u_{user}__"
    file1 = minio_files[0]
    job_info = {
        "id": "tstjob1",
        "namespace_prefix": namespace_prefix,
        "outputs": [{"file": file1, "crc64nvme": "fake"}]
    }
    sparkprov = None
    try:
        # run the importer
        sparkprov = SparkProvider("test_checkm2", user)
        run_import(sparkprov, job_info, {"deltatable": "checkm2_test.checkm2_single"})
        
        # might as well keep using the importer spark session. If needed make yet another
        # new session
        spark = sparkprov.spark
        # check the results
        res_df = spark.sql(f"SELECT * FROM {namespace_prefix}checkm2_test.checkm2_single")
        actual_data = res_df.orderBy(CHECKM2_NAME_FIELD).collect()
        assert_pyspark_rows_almost_equal(
            actual_data, expected_data_to_rows(_CHECKM2_EXPECTED_DATA_FILE1)
        )
    except Exception:
        traceback.print_exc()  # can get shadowed by exception in the finally block
        raise
    finally:
        if sparkprov:
            sparkprov.stop()
        spark_clean = spark_session("test_checkm2_helper", user)
        spark_clean.sql(f"DROP DATABASE IF EXISTS {namespace_prefix}checkm2_test CASCADE")
        spark_clean.stop()


def test_checkm2_success_2_files_existing_table(minio_files):
    # This test is slooow. Probably better to make a fixture that sets everything up and 
    # reuse it for other tests.
    user = "testuser"
    namespace_prefix = f"u_{user}__"
    file1, file2 = minio_files
    job_info = {
        "id": "tstjob2",
        "namespace_prefix": namespace_prefix,
        "outputs": [
            {
                "file": file1,
                "crc64nvme": "fake",
            },
            {
                "file": f"{get_CTS_output_bucket()}/checkm2/sub1/other_checkm_file",
                "crc64nvme": "fake",
            },
            {
                "file": file2,
                "crc64nvme": "fake",
            },
        ]
    }
    spark_setup = spark_session("test_checkm2_startup", user)
    sparkprov = None
    try:
        spark_setup.sql(f"CREATE DATABASE IF NOT EXISTS {namespace_prefix}checkm2_test")
        df = spark_setup.createDataFrame(_CHECKM2_DB_INIT_DATA, schema=CHECKM2_DB_SCHEMA)
        df.write.mode(
            "overwrite"
            ).option("compression", "snappy"
            ).format("delta"
            ).saveAsTable(f"{namespace_prefix}checkm2_test.checkm2"
        )
        spark_setup.stop()

        # run the importer
        sparkprov = SparkProvider("test_checkm2", user)
        run_import(sparkprov, job_info, {"deltatable": "checkm2_test.checkm2"})
        
        # might as well keep using the importer spark session. If needed make yet another
        # new session
        spark = sparkprov.spark
        # check the results
        res_df = spark.sql(f"SELECT * FROM {namespace_prefix}checkm2_test.checkm2")
        actual_data = res_df.orderBy(CHECKM2_NAME_FIELD).collect()
        assert_pyspark_rows_almost_equal(
            actual_data, expected_data_to_rows(_CHECKM2_EXPECTED_DATA_FULL)
        )
    except Exception:
        traceback.print_exc()  # can get shadowed by exception in the finally block
        raise
    finally:
        spark_setup.stop()
        if sparkprov:
            sparkprov.stop()
        spark_clean = spark_session("test_checkm2_helper", user)
        spark_clean.sql(f"DROP DATABASE IF EXISTS {namespace_prefix}checkm2_test CASCADE")
        spark_clean.stop()
