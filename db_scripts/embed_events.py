# Databricks notebook source
# MAGIC %pip install python-dotenv

# COMMAND ----------

# MAGIC
# MAGIC %pip install -r ../ml.requirements.txt

# COMMAND ----------

import logging
from importlib import reload
import pandas as pd
from typing import Iterable
import pyspark.sql as ss
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from data_proc.utils import is_not_na

import data_proc.widget_helper as wh
reload(wh)

logging.getLogger().setLevel("WARN")
wh.set_up_date_range_widgets(spark)



# COMMAND ----------

start_date, end_date = wh.get_date_range(spark)

print(f"DATE RANGE: {start_date} => {end_date}")

# COMMAND ----------

raw_events_df = (
    spark.read.table("gdelt.silver_events")
    .filter(F.col("date_added").between(start_date, end_date))
).cache()

# COMMAND ----------

events_sf_2 = (
    raw_events_df
    .withColumn(
        "picked_fields", F.struct([
                                    col("a1_name"), col("a1_type1_desc"), 
                                    col("ev_root_desc"), 
                                    col("a2_name"), col("a2_type1_desc"),
                                    col("ev_desc"), 
                                    col("action_geo_country"), 
                                    col("action_geo_state"), 
                                    col("action_geo_location"), 
                                    col("source_url"),
        ])
    )
)

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

    action_geo_full_name = f"{event['action_geo_country']} / {event['action_geo_state']} / {event['action_geo_location']}"
    geo_full_part = f" This took place in: {action_geo_full_name}"
    source_part = f" and was reported in: {event['source_url']}"

    return f"{actor1_part} {action_part} {actor2_part} {geo_full_part} {source_part}"

from pyspark.sql.types import StringType

svp_desc_udf = F.udf(svp_description_for_event, StringType())

# COMMAND ----------

events_sf_3 = (
    events_sf_2
      .select("date_added", "ev_id", "ev_date", "picked_fields")
      .withColumn("svp_description", svp_desc_udf(col("picked_fields")))
      .drop("picked_fields")
).cache()

# COMMAND ----------

from sentence_transformers import SentenceTransformer

EMBEDDER = SentenceTransformer("all-MiniLM-L6-v2")

def _embed_df_iter(pd_dfs: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    def _embed_df(pd_df: pd.DataFrame) -> pd.DataFrame:        
        ret_df = pd_df.copy()
        ret_df['svp_embedding'] = list(EMBEDDER.encode(pd_df["svp_description"]))

        return ret_df

    for pd_df in pd_dfs:
        yield _embed_df(pd_df)


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType, ArrayType, FloatType

EMBED_RESULT_SCHEMA = StructType(
    [
        StructField("date_added", DateType(), nullable=False),
        StructField("ev_id", IntegerType(), nullable=False),
        StructField("ev_date", DateType(), nullable=False),
        StructField("svp_description", StringType(), nullable=False),
        StructField("svp_embedding", ArrayType(FloatType()), nullable=False),
    ]
)

embed_results = (
    events_sf_3
    .limit(1000) # TODO: remove limit
    .mapInPandas(
        _embed_df_iter,
        schema=EMBED_RESULT_SCHEMA
    )
).cache()

# COMMAND ----------

embed_results.limit(1000).show()
