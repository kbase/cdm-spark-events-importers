"""
Importer for Checkm2 quality metrics.
"""

from delta.tables import DeltaTable
import logging
from pathlib import Path
from pyspark.sql.functions import col, lit
from typing import Any

from cdmeventimporters import utilities


_QUAL_REP = "quality_report.tsv"
_JOB_ID_COL = "cts_job_id"


def run_import(get_spark, job_info: dict[str, Any], override_metadata: dict[str, Any] = None):
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
    override_metadata - instead of loading data from the checkm2.yaml file, use this metadata
        instead. Useful for testing. The events processor will never use it.
    """
    
    # If there are different versions of the image that require different processing,
    # check the image digest (job_info["image_digest"]) and pick which code to run based on that.
    logr = logging.getLogger(__name__)

    # TODO DOCS document job_info structure
    job_id = job_info["id"]
    output_files = [f["file"] for f in job_info["outputs"] if f["file"].endswith(_QUAL_REP)]
    logr.info(f"Importing {len(output_files)} CheckM2 quality reports from CTS job {job_id}")
    
    meta = utilities.get_importer_metadata((Path(__file__) / ".." / "checkm2.yaml").resolve())
    deltaname = meta["deltatable"]
    if override_metadata:
        olddelta = deltaname
        deltaname = override_metadata["deltatable"]
        logr.info(f"Overriding configured deltatable, {olddelta}, with {deltaname}")
    
    # For now just using 1 core per executor, this is mostly IO. If we want to be able
    # to set cores probably need output file size and input file count info
    spark = get_spark()
    # TODO CODE could probably add some helper functions for some of this
    delta_schema = DeltaTable.forName(spark, deltaname).toDF().schema
    schema_fields = {f.name for f in delta_schema}
    if _JOB_ID_COL not in schema_fields:
        raise ValueError(
            f"The expected job id column, {_JOB_ID_COL}, is not in the configured "
            + f"CheckM2 deltatable, {deltaname}"
    )
    df = spark.read.option(
        "header", True
        ).option("sep", "\t"
        ).csv([f"s3a://{f}" for f in output_files]
        ).withColumn(_JOB_ID_COL, lit(job_id)
    )
    got_fields = set(df.columns)
    if schema_fields - got_fields:
        raise ValueError(
            f"Missing expected columns in the CheckM2 data: {schema_fields - got_fields}")

    # coerce schema to match deltatable, needs to happen AFTER the column existence check
    columns = [
        (col(field.name).cast(field.dataType)).alias(field.name)
        for field in delta_schema
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
