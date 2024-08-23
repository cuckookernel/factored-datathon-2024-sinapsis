"""Databricks sql connection helpers"""
import os
from typing import Optional, TypeAlias

import pandas as pd
from pandas import DataFrame
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql.session import SparkSession

# %%
GenericDF  = DataFrame | SparkDF

def get_sql_conn() -> "databricks.sql.Connection":
    """Get connection details from env var and"""
    from databricks import sql

    conn_details = os.environ["DATABRICKS_SQL_CONN_DETAILS"]
    parts = conn_details.split(";")
    if len(parts) != 3: # noqa: PLR2004
        raise ValueError("Expected ;-separated string consisting of exactly three parts, "
                         f"but contains {len(parts)}")

    return sql.connect(server_hostname=parts[0],
                       http_path=parts[1],
                       access_token=parts[2])
# %%



def run_query(query_sql: str, spark: Optional[SparkSession] = None) -> GenericDF:
    """Run a query either getting a connection or directly via spark context"""
    if spark is None: # This means we are running in local

        db_conn = get_sql_conn()
        results_df = pd.read_sql(query_sql, db_conn)
        db_conn.close()
    else:
        return spark.sql(query_sql)

    return results_df
