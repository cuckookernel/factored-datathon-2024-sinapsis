# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

import logging
import sys
from collections.abc import Iterable
from datetime import date
from importlib import reload
from pathlib import Path

import pandas as pd
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

print("CWD:", Path.cwd())
sys.path.append("../")
import data_proc.common as com
import data_proc.job_helper as jh
import data_proc.news.scraping as scr

reload(scr)
reload(com)
reload(jh)

# creating aliases to avoid undefined name errors from ruff
# noinspection PyUnboundLocalVariable
spark = spark_ = spark   # noqa: F821, PLW0127   # type: ignore [name-defined]
logging.getLogger().setLevel("WARN")

# COMMAND ----------

print(scr.HEATED_EVENTS_SQL_TMPL)

# COMMAND ----------

# heat_date = get_param_or_default(spark, " date(2023, 8, 10)
start_date = jh.get_param_or_default(spark, "start_date", date(2023, 8, 23), com.try_parse_date)
end_date = jh.get_param_or_default(spark, "end_date", date(2023, 8, 23), com.try_parse_date)
lookback_days = jh.get_param_or_default(spark, "lookback_days", 1, int)
ev_heat_table = jh.get_param_or_default(spark, "ev_heat_table",
                                        "gdelt.heat_indicator_by_event_dummy_teo")
top_k = jh.get_param_or_default(spark,  "top_k", 1, int)

start_date, end_date = jh.get_date_range_from_values(start_date, end_date, lookback_days)

print("params:", {"start_date": start_date, "end_date": end_date,
                  "ev_heat_table": ev_heat_table, "top_k": top_k})

top_news_events = scr.get_most_heated_events_spark(
            spark,
            ev_heat_table=ev_heat_table,
            start_date=start_date,
            end_date=end_date,
            top_k=top_k
).cache()

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

scrape_results = top_news_events.withColumnRenamed("heat_date", "part_date").mapInPandas(
    _scrape_from_df_iter,
    schema=scrape_result_schema
).cache()

# COMMAND ----------

scrape_results.limit(10).display()

# COMMAND ----------

spark.sql("refresh table gdelt.scraping_results")

# COMMAND ----------

(scrape_results
    .write.mode("overwrite")
    .option("replaceWhere", f"part_date >= '{start_date} and part_date <= '{end_date}'")
    .partitionBy("part_date")
    .saveAsTable("gdelt.scraping_results"))

# COMMAND ----------
