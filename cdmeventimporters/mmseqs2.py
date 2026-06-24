"""
Importer for MMseqs2 easy-cluster results.
"""

import logging
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, StructField, StructType

from cdmeventimporters import utilities


CTS_JOB_ID = "cts_job_id"
_CLUSTER_TSV = "cluster_results_cluster.tsv"

MMSEQS2_CLUSTER_SCHEMA = StructType([
    StructField("representative", StringType()),
    StructField("member", StringType()),
    StructField(CTS_JOB_ID, StringType()),
])


def _ensure_table(spark: SparkSession, logr: logging.Logger, full_tablename: str):
    namespace = full_tablename.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    if not spark.catalog.tableExists(full_tablename):
        logr.info(f"Creating new Iceberg table {full_tablename}")
        empty_df = spark.createDataFrame([], MMSEQS2_CLUSTER_SCHEMA)
        empty_df.writeTo(full_tablename).using("iceberg").create()


def run_import(get_spark, job_info: dict[str, Any], metadata: dict[str, Any]):
    """
    Run the MMseqs2 easy-cluster import.

    Reads cluster_results_cluster.tsv (no header, columns: representative, member)
    and writes to an Iceberg table with the cts_job_id appended.

    Assumptions:
        * The cluster TSV has no header and two tab-separated columns.
        * Each (representative, member, cts_job_id) triple is unique - duplicate events
          from reprocessing are safe due to the merge condition.
        * Results from different jobs are kept separately (cts_job_id is part of the key).

    get_spark - a function to get a spark session. Has a single keyword argument,
        executor_cores, that sets the cores per spark executor for the job. Defaults to 1.
    job_info - information about the completed CTS job, in particular the job ID and
        output files.
    metadata - importer metadata from the YAML config (must include 'table').
    """
    logr = logging.getLogger(__name__)

    job_id = job_info["id"]
    output_files = [f["file"] for f in job_info["outputs"] if f["file"].endswith(_CLUSTER_TSV)]
    if not output_files:
        raise ValueError("No MMseqs2 cluster TSV files found in job outputs")
    logr.info(f"Importing {len(output_files)} MMseqs2 cluster file(s) from CTS job {job_id}")

    tablename = metadata.get("table")
    if not tablename:
        raise ValueError(
            "Expected a 'table' key in the importer metadata with the namespace.table as the value"
        )
    # The Spark session is wired to the user's Iceberg catalog as the default catalog,
    # so a `<namespace>.<table>` reference resolves into that catalog automatically.

    spark = get_spark()
    _ensure_table(spark, logr, tablename)

    df = spark.read.option("header", False).option("sep", "\t").csv(
        [f"s3a://{f}" for f in output_files]
    ).toDF("representative", "member").withColumn(CTS_JOB_ID, lit(job_id))

    columns = [
        col(field.name).cast(field.dataType).alias(field.name)
        for field in MMSEQS2_CLUSTER_SCHEMA
    ]
    df = df.select(*columns)

    # Deduplication key includes cts_job_id so results from different jobs coexist,
    # but reprocessed events for the same job don't insert duplicate rows.
    utilities.merge_spark_df_to_table(
        spark,
        df,
        tablename,
        "target.representative = source.representative "
        "AND target.member = source.member "
        "AND target.cts_job_id = source.cts_job_id",
        update=False,
    )
