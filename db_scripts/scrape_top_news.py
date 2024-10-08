# Databricks notebook source
# MAGIC %pwd

# COMMAND ----------

# %ls
# %ls ../
%pip install -r ../requirements.txt
# %pip install ../   # install data_proc and shared modules directly from our repo source

# COMMAND ----------

# %pip install ../ -t /local_disk0/.ephemeral_nfs/cluster_libraries/python/lib/python3.11/site-packages

# COMMAND ----------

import sys
sys.path

# COMMAND ----------

import logging
import sys
from collections.abc import Iterable
from datetime import date
from importlib import reload
from pathlib import Path

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

import data_proc.common as com
import data_proc.job_helper as jh
import data_proc.news.scraping as scr

from data_proc.widget_helper import set_up_date_range_widgets, get_date_range

print("data_proc.common at: ", com.__file__)
reload(scr)
reload(com)
reload(jh)

logging.getLogger().setLevel("WARN")
# creating aliases to avoid undefined name errors from ruff
# noinspection PyUnboundLocalVariable
spark = spark_ = spark   # noqa: F821, PLW0127   # type: ignore [name-defined]


# COMMAND ----------

# heat_date = get_param_or_default(spark, " date(2023, 8, 10)
set_up_date_range_widgets(spark)
start_date, end_date = get_date_range(spark)
ev_heat_table = jh.get_param_or_default(spark, "ev_heat_table",
                                        "gdelt.heat_indicator_by_event")
top_k = jh.get_param_or_default(spark,  "top_k", 1, int)

print("params:", {"start_date": start_date, "end_date": end_date,
                  "ev_heat_table": ev_heat_table, "top_k": top_k})
reload(scr)
top_news_events = scr.get_most_heated_events_spark(
    spark,
    ev_heat_table=ev_heat_table,
    start_date=start_date,
    end_date=end_date,
    top_k=top_k
).cache()

# COMMAND ----------

top_news_events.cache().groupby("date_added").agg(F.count("ev_id")).show()

# COMMAND ----------

top_news_events.limit(100).display()

# COMMAND ----------


def _scrape_from_df_iter(pd_dfs: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    def _scrape_from_df(pd_df: pd.DataFrame) -> pd.DataFrame:
        return pd_df.apply(scr.scrape_one, axis=1)

    for df in pd_dfs:
        yield _scrape_from_df(df)

# COMMAND ----------

scrape_result_schema = StructType([
    StructField('source_url', StringType(), True),
    StructField('scraped_text', StringType(), True),
    StructField('status_code', LongType(), True),
    StructField('url_hash', StringType(), True),
    StructField('scraped_len', LongType(), True),
    StructField('scraped_text_len', LongType(), True),
    StructField('request_err', StringType(), True),
    StructField('part_date', DateType(), True)
])

scraping_results = (
    top_news_events
    .mapInPandas(
        _scrape_from_df_iter,
        schema=scrape_result_schema
    )
).cache()

# COMMAND ----------

(scraping_results
   .groupby("part_date")
   .agg(F.count(F.col("source_url")))
).show()

# COMMAND ----------

spark.sql("refresh table gdelt.scraping_results")

# COMMAND ----------

from datetime import date

output_table = "gdelt.scraping_results"

actual_date_range = scraping_results.agg(F.min(F.col("part_date")).alias("min_date"),  
                                         F.max(F.col("part_date")).alias("max_date")
                    ).collect()
actual_date_range_row = actual_date_range[0]
print(f"ACTUAL DATE RANGE: {actual_date_range_row.min_date} to {actual_date_range_row.max_date}")

spark.sql(f"delete from {output_table} where part_date >= '{actual_date_range_row.min_date}' AND part_date <= '{actual_date_range_row.max_date}'")
(scraping_results
    .write
    .mode("append")
    .partitionBy("part_date")    
    .saveAsTable(output_table))

# COMMAND ----------


