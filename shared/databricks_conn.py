"""Databricks sql connection helpers"""
import os

from databricks import sql
from databricks.sql.client import Connection

# %%


def get_sql_conn() -> Connection:
    """Get connection details from env var and"""
    conn_details = os.environ["DATABRICKS_SQL_CONN_DETAILS"]
    parts = conn_details.split(";")
    if len(parts) != 3:
        raise ValueError("Expected ;-separated string consisting of exactly three parts, "
                         f"but contains {len(parts)}")

    return sql.connect(server_hostname=parts[0],
                       http_path=parts[1],
                       access_token=parts[2])
# %%
