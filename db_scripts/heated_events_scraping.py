# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt
"""Databricks script for scraping of most heated events"""

# COMMAND ----------

import logging
from collections.abc import Iterable
from datetime import date
from importlib import reload

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

import data_proc.news.local_scraping_most_heated_events as scmh
from data_proc.news.scraping import get_most_heated_events_spark

logging.getLogger().setLevel("WARN")


# COMMAND ----------

spark_ = spark  # noqa: F821  # avoid undefined name errors from linter
display_ = display # noqa: F821  # avoid undefined name errors from linter
heat_date = date(2023, 8, 11)
top_k = 3

# COMMAND ----------


heated_events = get_most_heated_events_spark(spark_,
                                             heat_date=heat_date,
                                             top_k=top_k)

# COMMAND ----------
display_(heated_events.limit(100))

# COMMAND ----------


reload(scmh)

def _scrape_from_df_iter(pd_dfs: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    def _scrape_from_df(pd_df: pd.DataFrame) -> pd.DataFrame:
        return pd_df.apply(scmh.scrape_one, axis=1)

    for df in pd_dfs:
        yield _scrape_from_df(df)

# COMMAND ----------

results_table = spark_.table("gdelt.scraping_results")
print(results_table.schema)
results_table.limit(10).collect()

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
scrape_results = (heated_events
                  .mapInPandas(_scrape_from_df_iter, schema=scrape_result_schema)
                  .withColumn('part_date', F.lit(heat_date))
            )

# COMMAND ----------
display_(scrape_results.limit(10))

# COMMAND ----------

spark_.sql("refresh table gdelt.scraping_results")
# COMMAND ----------

(scrape_results
    .write.mode("overwrite")
    .option("replaceWhere", f"part_date == '{heat_date}'")
    .partitionBy("part_date")
    .saveAsTable("gdelt.scraping_results"))


# COMMAND ----------
