"""Event aggregation related helpers.

Se also views defined under data_proc/views/event_aggregations.sql
"""
import pandas as pd

from data_proc.common import gdelt_base_data_path
from shared.databricks_conn import get_sql_conn

# %%

def download_event_aggregations() -> None:
    """Download aggregated event tables to local parquets."""
    # %%
    conn = get_sql_conn()
    data_dir = gdelt_base_data_path()
    # %%
    data_df = pd.read_sql("SELECT * from gdelt.events_agg_country_quad_class",
                          con=conn)

    data_df.to_parquet(f"{data_dir}/events_agg_country_quad_class.parquet")
    # %%
    data_df = pd.read_sql("SELECT * from gdelt.events_agg_country_ev_root",
                          con=conn)
    data_df.to_parquet(f"{data_dir}/events_agg_country_ev_root.parquet")
    # %%
