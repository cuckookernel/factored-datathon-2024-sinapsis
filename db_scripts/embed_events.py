# Databricks notebook source
# MAGIC %pip install python-dotenv

# COMMAND ----------

# MAGIC
# MAGIC %pip install -r ../ml.requirements.txt

# COMMAND ----------

import logging
import pandas as pd
from typing import Iterable
import pyspark.sql as ss
import pyspark.sql.functions as F
from data_proc.utils import is_not_na

from data_proc.widget_helper import set_up_date_range_widgets, get_date_range

logging.getLogger().setLevel("WARN")
set_up_date_range_widgets(spark)



# COMMAND ----------

start_date, end_date = get_date_range(spark)

print(f"DATE RANGE: {start_date} => {end_date}")

# COMMAND ----------


def svp_description_for_event(event: ss.Row) -> str:
    """Create a full description of the form below

    (Actor 1) (action) (Actor2) this happend in (geo_full_name), was reported in (source_url)
    """
    if is_not_na(event['a1_name']):
        actor1_pieces = [
                f"Actor 1: {event['a1_name']}" if is_not_na(event['a1_name']) else "",
                f"(type: {event['a1_type1_desc']} )" if is_not_na(event['a1_type1_desc']) else "",
        ]
        actor1_part = " ".join(actor1_pieces).strip()
    else:
        actor1_part = ""

    action_part = f"performed action {event['ev_root_desc']}, more specifically {event['ev_desc']}"

    if is_not_na(event['a2_name']):
        actor2_pieces = [
                f"on Actor 2: {event['a2_name']}" if is_not_na(event['a2_name']) else "",
                f"(type: {event['a2_type1_desc']})" if is_not_na(event['a2_type1_desc']) else "",
        ]
        actor2_part = " ".join(actor2_pieces).strip()
    else:
        actor2_part = ""

    geo_full_part = f" This took place in: {event['action_geo_full_name']}"
    source_part = f" and was reported in: {event['source_url']}"

    return f"{actor1_part} {action_part} {actor2_part} {geo_full_part} {source_part}"



# COMMAND ----------

raw_events_df = (
    spark.read.table("gdelt.silver_events")
)

# COMMAND ----------



def _embed_df_iter(pd_dfs: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    def _embed_df(pd_df: pd.DataFrame) -> pd.DataFrame:
        return pd_df.apply(scr.embed_one, axis=1)

    for df in pd_dfs:
        yield _embed_from_df(df)

# COMMAND ----------
