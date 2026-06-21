"""
Importer for KofamScan KO HMM annotations.

Stores only significant hits (rows with `*` in the leading column, meaning
`score >= per-KO threshold`). Each gene can have multiple significant KO hits;
one row per (gene, KO).
"""

import logging
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DoubleType, FloatType, StringType, StructField, StructType

from cdmeventimporters import utilities


CTS_JOB_ID = "cts_job_id"
_ANNOTATIONS_TSV = "annotations.tsv"


KOFAMSCAN_DB_SCHEMA = StructType([
    StructField("gene_name", StringType()),
    StructField("KO", StringType()),
    StructField(CTS_JOB_ID, StringType()),
    StructField("threshold", FloatType()),
    StructField("score", FloatType()),
    StructField("evalue", DoubleType()),
    StructField("KO_definition", StringType()),
])


def _ensure_table(spark: SparkSession, logr: logging.Logger, full_tablename: str):
    namespace = full_tablename.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    if not spark.catalog.tableExists(full_tablename):
        logr.info(f"Creating new Iceberg table {full_tablename}")
        empty_df = spark.createDataFrame([], KOFAMSCAN_DB_SCHEMA)
        empty_df.writeTo(full_tablename).using("iceberg").create()


def run_import(get_spark, job_info: dict[str, Any], metadata: dict[str, Any]):
    """
    Run the KofamScan import.

    Reads each `annotations.tsv` from the job's outputs and writes the significant KO
    hits into an Iceberg table keyed on (gene_name, KO, cts_job_id). Filter is applied
    inside this importer: rows whose first column is not `*` are dropped (non-significant
    HMM hits below the KO-specific score threshold).

    KofamScan output format (one file per genome, tab-separated):
        line 0  : `#  gene name  KO  thrshld  score  E-value  "KO definition"`
        line 1  : `#  ---------  ------  ...` (separator row, naturally filtered out by
                  the significance check below)
        line 2+ : data rows, with `*` in column 0 for significant hits

    Assumptions
        * The schema is uniform across kofamscan versions.
        * Multiple significant hits per gene are valid (different KOs can hit; we keep
          all of them).
        * Dedup key is (gene_name, KO, cts_job_id) so results from different jobs coexist
          and reprocessed events for the same job do not insert duplicates.

    get_spark - a function to get a spark session. Has a single keyword argument,
        executor_cores, that sets the cores per spark executor for the job. Defaults to 1.
    job_info - information about the completed CTS job, in particular the job ID and
        output files.
    metadata - importer metadata from the YAML config (must include 'table').
    """
    logr = logging.getLogger(__name__)

    job_id = job_info["id"]
    output_files = [f["file"] for f in job_info["outputs"] if f["file"].endswith(_ANNOTATIONS_TSV)]
    if not output_files:
        raise ValueError("No KofamScan annotations.tsv files found in job outputs")
    logr.info(f"Importing {len(output_files)} KofamScan annotation file(s) from CTS job {job_id}")

    tablename = metadata.get("table")
    if not tablename:
        raise ValueError(
            "Expected a 'table' key in the importer metadata with the namespace.table as the value"
        )
    # The Spark session is wired to the user's Iceberg catalog as the default catalog,
    # so a `<namespace>.<table>` reference resolves into that catalog automatically.

    spark = get_spark()
    _ensure_table(spark, logr, tablename)

    # Read with header=True (so Spark consumes line 0 as column names), then rename
    # positionally with toDF so we sidestep the `#` and `gene name` special-character
    # issues. The separator row (line 1 in the source file) becomes data row 0 here;
    # the significance filter below excludes it because its first column is `#---------`
    # rather than `*`.
    raw = spark.read.option("header", True).option("sep", "\t").csv(
        [f"s3a://{f}" for f in output_files]
    ).toDF("significant", "gene_name", "KO", "threshold", "score", "evalue", "KO_definition")

    df = (raw
          .filter(col("significant") == "*")
          .withColumn(CTS_JOB_ID, lit(job_id)))

    columns = [
        col(field.name).cast(field.dataType).alias(field.name)
        for field in KOFAMSCAN_DB_SCHEMA
    ]
    df = df.select(*columns)

    utilities.merge_spark_df_to_table(
        spark,
        df,
        tablename,
        "target.gene_name = source.gene_name "
        "AND target.KO = source.KO "
        "AND target.cts_job_id = source.cts_job_id",
        update=False,
    )
