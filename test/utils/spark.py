"""
Utilities around creating a spark session for testing.
"""

# similar to https://github.com/kbase/cdm-spark-events/blob/main/cdmsparkevents/spark.py

import logging
from pathlib import Path
import os

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


_REQUIRED_JAR_PREFIXES = ["delta-spark_", "hadoop-aws-"]


def _find_jars():
    logr = logging.getLogger(__name__)
    directory = Path(os.environ["IMP_SPARK_JARS_DIR"]).resolve()
    if not directory.is_dir():
        raise ValueError(f"Provided spark jars path is not a directory: {directory}")
    
    results = []

    for prefix in _REQUIRED_JAR_PREFIXES:
        matches = list(directory.glob(f"{prefix}*.jar"))
        if len(matches) != 1:
            raise ValueError(
                f"Expected exactly one JAR for prefix '{prefix}', found {len(matches)}"
            )
        jar = str(matches[0].resolve())
        logr.info(f"Found jar {jar}")
        results.append(jar)
    
    return ", ".join(results)


_JARS = _find_jars()


def spark_session(app_name: str, executor_cores: int = 1) -> SparkSession:
    """
    Generate a spark session for an importer.
    
    app_name - The name for the spark application. This should be unique among applications.
    executor_cores - the number of cores to use per executor.
    """
    if not app_name or not app_name.strip():
        raise ValueError("app_name cannot be whitespace only")
    config = {
        # Basic config
        "spark.app.name": app_name,
        # Overrides base image configuration
        "spark.executor.cores": str(executor_cores),
        "spark.driver.host": os.environ["IMP_SPARK_DRIVER_HOST"],
        "spark.master": os.environ["IMP_SPARK_MASTER_URL"],
        "spark.jars": _JARS,
        
        # Dynamic allocation is set up in the base image setup.sh script

        # S3 setup
        "spark.hadoop.fs.s3a.endpoint": os.environ["IMP_MINIO_URL"],
        "spark.hadoop.fs.s3a.access.key": os.environ["IMP_MINIO_ACCESS_KEY"],
        "spark.hadoop.fs.s3a.secret.key": os.environ["IMP_MINIO_SECRET_KEY"],
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    
        # Deltalake setup
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        "spark.sql.catalogImplementation": "hive",
        
        # Hive & S3 warehouse dir config is set up in the base image
    }
    
    spark_conf = SparkConf().setAll(list(config.items()))

    # Initialize SparkSession
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


class SparkProvider:
    """
    This class presents the same functional API as the function provided to an importer's
    `run_import` method to get a spark session. It additionally allows for stopping the
    session when the importer is complete, which the event processor normally does automatically.
    In the context of tests, any tests will need to stop the session to release resources
    on the spark workers.
    """
    
    def __init__(self, app_name: str):
        """
        Create the spark provider.
        
        app_name - The name for the spark application. This should be unique among applications.
         """
        if not app_name or not app_name.strip():
            raise ValueError("app_name cannot be whitespace only")
        self.app_name = app_name
        self.spark = None
    
    def __call__(self, *, executor_cores: int = 1) -> SparkSession:
        """
        Create a spark session.
        
        executor_cores - the number of cores to use per executor.
        """
        self.spark = spark_session(self.app_name, executor_cores=executor_cores)
        return self.spark
    
    def stop(self):
        """
        Stop the spark session if it exists.
        """
        if self.spark:
            self.spark.stop()
