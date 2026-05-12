"""
General utilities useful for importing data into the CDM.
"""

import uuid

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from cdmeventimporters.arg_checkers import (
    not_falsy as _not_falsy,
    require_string as _require_string,
)


def merge_spark_df_to_table(
    spark: SparkSession,
    df: DataFrame,
    full_table_name: str,
    merge_condition: str,
    update: bool = False,
    target: str = "target",
    source: str = "source",
):
    """
    Merge a Spark DataFrame into an existing table using SQL `MERGE INTO`. Works with
    any catalog that supports SQL merge (e.g. Iceberg via Polaris).

    spark - a SparkSession configured with the target table's catalog.
    df - the DataFrame to merge. Its schema must match that of the table.
    full_table_name - the name of the table, in `<namespace>.<table>` format (or
        `<catalog>.<namespace>.<table>` if not relying on the default catalog).
    merge_condition - the condition that detects equivalent rows where the row in
        the dataframe should be dropped if it already exists in the table. For
        example, `"target.employee_id = source.employee_id"`.
    update - instead of dropping equivalent rows in the dataframe, replace the rows
        in the table with the row in the dataframe.
    target - the alias of the target table to use in the merge condition string.
    source - the alias of the source dataframe to use in the merge condition string.
    """
    _not_falsy(spark, "spark")
    _not_falsy(df, "df")
    _require_string("full_table_name", full_table_name)
    _require_string("merge_condition", merge_condition)
    _require_string("target", target)
    _require_string("source", source)

    source_view = f"merge_source_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(source_view)
    matched_clause = "WHEN MATCHED THEN UPDATE SET *" if update else ""
    try:
        spark.sql(f"""
            MERGE INTO {full_table_name} {target}
            USING {source_view} {source}
            ON {merge_condition}
            {matched_clause}
            WHEN NOT MATCHED THEN INSERT *
        """)
    finally:
        spark.catalog.dropTempView(source_view)
