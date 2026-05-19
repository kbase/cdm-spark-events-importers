"""
Utilities around creating a spark session for testing.
"""

# similar to https://github.com/kbase/cdm-spark-events/blob/main/cdmsparkevents/spark.py

import logging
import os
from pathlib import Path

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


_ICEBERG_CATALOG_ALIAS = "my"
_REQUIRED_JAR_PREFIXES = ["iceberg-spark-runtime-", "hadoop-aws-"]


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


def _personal_catalog_name(user: str) -> str:
    template = os.environ.get("IMP_POLARIS_PERSONAL_CATALOG_TEMPLATE", "user_{user}")
    if "{user}" not in template:
        raise ValueError("IMP_POLARIS_PERSONAL_CATALOG_TEMPLATE must contain {user}")
    return template.format(user=user)


def _personal_catalog_aliases(personal_catalog: str) -> list[str]:
    aliases = [_ICEBERG_CATALOG_ALIAS]
    portable_alias = personal_catalog.strip()
    if portable_alias.startswith("user_"):
        portable_alias = portable_alias[len("user_"):]
    if portable_alias and portable_alias not in aliases:
        aliases.append(portable_alias)
    return aliases


def _polaris_credential() -> str:
    cred = os.environ.get("IMP_POLARIS_CREDENTIAL", "").strip()
    if cred:
        return cred
    cred_file = os.environ.get("IMP_POLARIS_CREDENTIAL_FILE")
    if not cred_file:
        raise ValueError(
            "Neither IMP_POLARIS_CREDENTIAL nor IMP_POLARIS_CREDENTIAL_FILE is set"
        )
    if not Path(cred_file).is_file():
        raise ValueError(f"Polaris credential file does not exist: {cred_file}")
    cred = Path(cred_file).read_text().strip()
    if not cred:
        raise ValueError(f"Polaris credential file is empty: {cred_file}")
    return cred


def _catalog_props(prefix: str, polaris_uri: str, personal_catalog: str) -> dict[str, str]:
    return {
        f"{prefix}": "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": polaris_uri,
        f"{prefix}.credential": _polaris_credential(),
        f"{prefix}.warehouse": personal_catalog,
        f"{prefix}.scope": "PRINCIPAL_ROLE:ALL",
        f"{prefix}.token-refresh-enabled": "true",
        f"{prefix}.client.region": "us-east-1",
        f"{prefix}.s3.endpoint": os.environ["IMP_MINIO_URL"],
        f"{prefix}.s3.access-key-id": os.environ["IMP_MINIO_ACCESS_KEY"],
        f"{prefix}.s3.secret-access-key": os.environ["IMP_MINIO_SECRET_KEY"],
        f"{prefix}.s3.path-style-access": "true",
        f"{prefix}.s3.region": "us-east-1",
    }


def spark_session(app_name: str, user: str, executor_cores: int = 1) -> SparkSession:
    """
    Generate a spark session for an importer.

    app_name - The name for the spark application. This should be unique among applications.
    user - the user running the spark session. Selects the per-user Polaris catalog.
    executor_cores - the number of cores to use per executor.
    """
    if not app_name or not app_name.strip():
        raise ValueError("app_name cannot be whitespace only")
    if not user or not user.strip():
        raise ValueError("user cannot be whitespace only")
    polaris_uri = os.environ["IMP_POLARIS_CATALOG_URI"].rstrip("/")
    personal_catalog = _personal_catalog_name(user)
    config = {
        # Basic config
        "spark.app.name": app_name,
        "spark.executor.cores": str(executor_cores),
        "spark.driver.host": os.environ["IMP_SPARK_DRIVER_HOST"],
        "spark.master": os.environ["IMP_SPARK_MASTER_URL"],
        "spark.jars": _JARS,

        # Dynamic allocation is set up in the base image setup.sh script

        # S3 setup for direct s3a:// reads of CTS outputs
        "spark.hadoop.fs.s3a.endpoint": os.environ["IMP_MINIO_URL"],
        "spark.hadoop.fs.s3a.access.key": os.environ["IMP_MINIO_ACCESS_KEY"],
        "spark.hadoop.fs.s3a.secret.key": os.environ["IMP_MINIO_SECRET_KEY"],
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",

        # Iceberg / Polaris setup
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": _ICEBERG_CATALOG_ALIAS,
    }
    for alias in _personal_catalog_aliases(personal_catalog):
        config.update(_catalog_props(f"spark.sql.catalog.{alias}", polaris_uri, personal_catalog))

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

    def __init__(self, app_name: str, user: str):
        """
        Create the spark provider.

        app_name - The name for the spark application. This should be unique among applications.
         """
        if not app_name or not app_name.strip():
            raise ValueError("app_name cannot be whitespace only")
        if not user or not user.strip():
            raise ValueError("user cannot be whitespace only")
        self.app_name = app_name
        self.user = user
        self.spark = None

    def __call__(self, *, executor_cores: int = 1) -> SparkSession:
        """
        Create a spark session.

        executor_cores - the number of cores to use per executor.
        """
        self.spark = spark_session(self.app_name, self.user, executor_cores=executor_cores)
        return self.spark

    def stop(self):
        """
        Stop the spark session if it exists.
        """
        if self.spark:
            self.spark.stop()
