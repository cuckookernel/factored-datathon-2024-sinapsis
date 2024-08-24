"""Databricks sql connection helpers"""
import os

import pandas as pd
from databricks import sql

# %%

def get_sql_conn() -> "databricks.sql.Connection":  # noqa: F821 # don't want
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



def run_query(query_sql: str) -> pd.DataFrame:
    """Run a query either getting a connection or directly via spark context"""
    db_conn = get_sql_conn()
    results_df = pd.read_sql(query_sql, db_conn)
    db_conn.close()

    return results_df
