"""Databricks sql connection helpers"""
import os
from typing import Optional, TypeAlias

import pandas as pd
from databricks import sql
from databricks.sql.client import Connection
from pandas import DataFrame

SparkSession: TypeAlias = None
# %%


def get_sql_conn() -> Connection:
    """Get connection details from env var and"""
    conn_details = os.environ["DATABRICKS_SQL_CONN_DETAILS"]
    parts = conn_details.split(";")
    if len(parts) != 3: # noqa: PLR2004
        raise ValueError("Expected ;-separated string consisting of exactly three parts, "
                         f"but contains {len(parts)}")

    return sql.connect(server_hostname=parts[0],
                       http_path=parts[1],
                       access_token=parts[2])
# %%



def run_query(query_sql: str, spark: Optional[SparkSession] = None) -> DataFrame:
    """Run a query either getting a connection or directly via spark context"""
    if spark is None:
        db_conn = get_sql_conn()
        results_df = pd.read_sql(query_sql, db_conn)
        db_conn.close()
    else:
        # TODO (Mateo): in a spark session do something else here...
        raise NotImplementedError("Spark session is not None...")

    return results_df
