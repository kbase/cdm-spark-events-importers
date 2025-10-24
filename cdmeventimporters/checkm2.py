"""
Importer for Checkm2 quality metrics.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.utils import AnalysisException
from typing import Any

from cdmeventimporters import utilities


CTS_JOB_ID = "cts_job_id"
CHECKM2_NAME_FIELD = "Name"
_QUAL_REP = "quality_report.tsv"


CHECKM2_DB_SCHEMA = StructType([
    StructField(CHECKM2_NAME_FIELD, StringType()),
    StructField(CTS_JOB_ID, StringType()),
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


def _ensure_table(spark: SparkSession, logr: logging.Logger, full_tablename: str):
    namespace = full_tablename.split(".")[0]  # assumes just namespace and table
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {namespace}")
    try:
        spark.sql(f"DESCRIBE TABLE {full_tablename}")
        # table exists, all done
    except AnalysisException:
        logr.info(f"Creating new Delta table {full_tablename}")
        empty_df = spark.createDataFrame([], CHECKM2_DB_SCHEMA)
        empty_df.write.format("delta").option("compression", "snappy").saveAsTable(full_tablename)


def run_import(get_spark, job_info: dict[str, Any], metadata: dict[str, Any]):
    """
    Run the CheckM2 import.
    
    Open questions:
        * How do we connect this data to other tables? All that CheckM2 gives us is the filename
          with extensions removed.
        * CheckM2 silently ignores input files that it can't process. Do we want to do
          something about that?
        * If a type in the input isn't coercible into the deltatable column type, spark will
          insert a null into the cell. I haven't found a simple setting to make spark throw
          an error instead, all the solutions seem to require a lot of work & compute checking
          every value in the input.
    
    Assumptions
        * We want all the data in the quality report. If that's not true we can drop columns.
        * We want to use the same column names as CheckM2.
        * We want to overwrite any duplicate filenames in the existing deltatable
            * If we want to not do that, make a composite key based on the job ID and the filename
              to keep results from all jobs but avoid duplicates due to events being resent
    
    get_spark - a function to get a spark session. Has a single keyword argument,
        executor_cores, that sets the cores per spark executor for the job. Defaults to 1.
    job_info - information about the job that ran, in particular the job ID and output files.
    metadata - the metadata, if any, from the importer's module file.
    """
    
    # If there are different versions of the image that require different processing,
    # check the image digest (job_info["image_digest"]) and pick which code to run based on that.
    logr = logging.getLogger(__name__)

    job_id = job_info["id"]
    output_files = [f["file"] for f in job_info["outputs"] if f["file"].endswith(_QUAL_REP)]
    if not output_files:
        raise ValueError("No checkm2 quality report files found")
    logr.info(f"Importing {len(output_files)} CheckM2 quality reports from CTS job {job_id}")
    # We could check the crc64nvmes here, but there's still the possibility of a race condition
    # between checking the crc and loading the file into the deltatable
    
    deltaname = metadata.get("deltatable")
    if not deltaname:
        raise ValueError(
            "Expected a 'deltatable' key in the importer metadata with the db table as the value"
        )
    deltaname = job_info["namespace_prefix"] + deltaname
    
    # For now just using 1 core per executor, this is mostly IO. If we want to be able
    # to set cores probably need output file size info
    spark = get_spark()
    # TODO CODE could probably add some helper functions for some of this
    _ensure_table(spark, logr, deltaname)
    schema_fields = {f.name for f in CHECKM2_DB_SCHEMA}
    df = spark.read.option(
        "header", True
        ).option("sep", "\t"
        ).csv([f"s3a://{f}" for f in output_files]
        ).withColumn(CTS_JOB_ID, lit(job_id)
    )
    got_fields = set(df.columns)
    if schema_fields - got_fields:
        raise ValueError(
            f"Missing expected columns in the CheckM2 data: {schema_fields - got_fields}")

    # coerce schema to match deltatable, needs to happen AFTER the column existence check
    columns = [
        (col(field.name).cast(field.dataType)).alias(field.name)
        for field in CHECKM2_DB_SCHEMA
    ]
    df = df.select(*columns)
    
    # now we load
    utilities.merge_spark_df_to_deltatable(
        spark,
        df,
        deltaname,
        "target.Name == source.Name",
        update=True,  # prevents duplicate data from entering the table
    )
