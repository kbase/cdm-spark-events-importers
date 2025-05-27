# TODO CHECKM2 add more test cases, failing tests etc.

from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, Row

from cdmeventimporters import checkm2
from utils.misc import (
    assert_pyspark_rows_almost_equal,
    get_CTS_output_bucket,
    get_s3_client,
    set_up_basic_logging,
    write_tsv_to_s3,
)
from utils.spark import spark_session, SparkProvider


_CTS_JOB_ID = "cts_job_id"
_CHECKM2_NAME_FIELD = "Name"


_CHECKM2_DB_SCHEMA = StructType([
    StructField(_CHECKM2_NAME_FIELD, StringType()),
    StructField(_CTS_JOB_ID, StringType()),
    StructField("Completeness", FloatType()),
    StructField("Contamination", FloatType()),
    StructField("Completeness_Model_Used", StringType()),
    StructField("Translation_Table_Used", IntegerType()),
    StructField("Coding_Density", FloatType()),
    StructField("Contig_N50", IntegerType()),
    StructField("Average_Gene_Length", FloatType()),
    StructField("Genome_Size", IntegerType()),
    StructField("GC_Content", FloatType()),
    StructField("Total_Coding_Sequences", IntegerType()),
    StructField("Total_Contigs", IntegerType()),
    StructField("Max_Contig_Length", IntegerType()),
    StructField("Additional_Notes", StringType(), True),  # allow nulls
])


_CHECKM2_FILE_HEADERS = [field.name for field in _CHECKM2_DB_SCHEMA if field.name != _CTS_JOB_ID]


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


_CHECKM2_EXPECTED_DATA = [
    ("GCA_bsubtilis", "tstjob", .91, .27, "mod", 9, .49, 150, .50, 46, .93, 36, 5, 16, "whoo"),
    ("GCA_ecoli", "tstjob", .90, .26, "model4", 8, .48, 149, .49, 45, .92, 35, 4, 15, None),
    ("GCA_keep", "old_job2", .88, .24, "model2", 6, .46, 147, .47, 43, .90, 33, 2, 13, "Lovely"),
    ("GCA_replace", "tstjob", .89, .25, "model3", 7, .47, 148, .48, 44, .91, 34, 3, 14, "hello"),
]


# Set force to True to see checkm2 logs. pytest -s or -o log_cli=true doesn't seem to work.
# Will cause a logging error to be thrown at the end of the tests
set_up_basic_logging(force=False)


def expected_data_to_rows():
    ret = []
    for r in _CHECKM2_EXPECTED_DATA:
        ret.append(Row(**{f: d for f, d in zip([field.name for field in _CHECKM2_DB_SCHEMA], r)}))
    return ret


def test_checkm2_success_2_files():
    # This test is slooow. Probably better to make a fixture that sets everything up and 
    # reuse it for other tests.
    bucket = get_CTS_output_bucket()
    file1 = f"{bucket}/checkm2/sub1/quality_report.tsv"
    file2 = f"{bucket}/checkm2/sub2/quality_report.tsv"
    job_info = {
        "id": "tstjob",
        "outputs": [
            {
                "file": file1,
                "crc64nvme": "fake",
            },
            {
                "file": f"{bucket}/checkm2/sub1/other_checkm_file",
                "crc64nvme": "fake",
            },
            {
                "file": file2,
                "crc64nvme": "fake",
            },
        ]
    }
    spark_setup = spark_session("test_checkm2_startup")
    sparkprov = None
    try:
        spark_setup.sql("CREATE DATABASE IF NOT EXISTS checkm2_test")
        df = spark_setup.createDataFrame(_CHECKM2_DB_INIT_DATA, schema=_CHECKM2_DB_SCHEMA)
        df.write.mode(
            "overwrite"
            ).option("compression", "snappy"
            ).format("delta"
            ).saveAsTable("checkm2_test.checkm2"
        )
        spark_setup.stop()
        s3cli = get_s3_client()
        write_tsv_to_s3(s3cli, file1, _CHECKM2_FILE_HEADERS, _CHECKM2_FILE1_DATA)
        write_tsv_to_s3(s3cli, file2, _CHECKM2_FILE_HEADERS, _CHECKM2_FILE2_DATA)

        # run the importer
        sparkprov = SparkProvider("test_checkm2")
        checkm2.run_import(
            sparkprov, job_info, override_metadata={"deltatable": "checkm2_test.checkm2"}
        )
        
        # might as well keep using the importer spark session. If needed make yet another
        # new session
        spark = sparkprov.spark
        # check the results
        res_df = spark.sql("SELECT * FROM checkm2_test.checkm2")
        actual_data = res_df.orderBy(_CHECKM2_NAME_FIELD).collect()
        assert_pyspark_rows_almost_equal(actual_data, expected_data_to_rows())
    finally:
        spark_setup.stop()
        if sparkprov:
            sparkprov.stop()
        spark_clean = spark_session("test_checkm2_helper")
        spark_clean.sql("DROP DATABASE IF EXISTS checkm2_test CASCADE")
        spark_clean.stop()
