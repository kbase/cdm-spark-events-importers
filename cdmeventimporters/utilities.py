"""
General utilities useful for importing data into the CDM.
"""

from delta.tables import DeltaTable
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import Any
import yaml

from cdmeventimporters.arg_checkers import (
    not_falsy as _not_falsy,
    require_string as _require_string
)


def merge_spark_df_to_deltatable(
        spark: SparkSession,
        df: DataFrame,
        full_table_name: str,
        merge_condition: str,
        update: bool = False,
        target: str = "target",
        source: str = "source",
    ):
    """
    Merge a Spark DataFrame into an existing deltatable.
    
    spark - a SparkSession configured with deltatable support.
    df - the DataFrame to merge. Its schema must match that of the table.
    full_table_name - the name of the table, in <database>.<table> format.
    merge_condition - the condition that will be used to detect equivalent rows where the row
        in the dataframe should be dropped if it already exists in the deltatable. For example,
        `"target.employee_id = source.employee_id"`
    update - instead of dropping equivalent rows in the dataframe, replace the rows in the
        deltatable with the row in the dataframe.
    target - the alias of the target deltatable to use in the merge condition string..
    source - the alias of the source dataframe to use in the merge condition string.
    """
    _not_falsy(spark, "spark")
    _not_falsy(df, "df")
    _require_string("full_table_name", full_table_name)
    _require_string("merge_condition", merge_condition)
    _require_string("target", target)
    _require_string("source", source)
    
    delta_table = DeltaTable.forName(spark, full_table_name)
    preex = delta_table.alias(
        target
        ).merge(source=df.alias(source), condition=merge_condition
        ).whenNotMatchedInsertAll(
    )
    if update:
        preex = preex.whenMatchedUpdateAll()
    preex.execute()


def get_importer_metadata(importer_yaml_path: Path) -> dict[str, Any]:
    """
    Read the importer yaml file specified and return the importer metadata embedded within it,
    or none if there is no metadata.
    """
    with open(importer_yaml_path) as f:
        data = yaml.safe_load(f)
    return data.get("importer_meta")
