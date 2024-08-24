# Databricks notebook source
# MAGIC
# MAGIC %pip install -r ../requirements.txt
# MAGIC """Databricks script for scraping of most heated events"""
# MAGIC import os
# MAGIC import sys
# MAGIC
# MAGIC # Might need this?
# MAGIC # sys.path.append("/Workspace/Repos/mateini@gmail.com/factored-datathon-2024-sinapsis")
# MAGIC print("CWD:", os.getcwd())
# MAGIC sys.path.append("../")
# MAGIC

# COMMAND ----------

import sys
import os
import logging
from collections.abc import Iterable
from datetime import date

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

from typing import Callable, TypeVar
from pyspark.sql.session import SparkSession
from pyspark.dbutils import DBUtils
import data_proc.news.scraping  as scr
from importlib import reload
reload(scr)
T_ = TypeVar("T_")

logging.getLogger().setLevel("WARN")

# creating aliases to avoid undefined name errors from ruff
spark = spark_ = spark   # noqa: F821   # type: ignore [name-defined]

# COMMAND ----------

import data_proc as dp
reload(dp)
import data_proc.common as com
import data_proc.job_helper as jh

reload(com)
com.try_parse_date

# COMMAND ----------

help(scr.get_most_heated_events_spark)

# COMMAND ----------


# heat_date = get_param_or_default(spark, " date(2023, 8, 10)
start_date = get_param_or_default(spark, "start_date", date(2023, 8, 23), try_parse_date)
end_date = get_param_or_default(spark, "end_date", date(2023, 8, 23), try_parse_date)
ev_heat_table = get_param_or_default(spark, "ev_heat_table", "heat_indicator_by_event_dummy_teo")
top_k = get_param_or_default(spark,  "top_k", 1, int)

print("params:", {"start_date": start_date, "end_date": end_date, "ev_heat_table": ev_heat_table, "top_k": top_k})

heated_events = scr.get_most_heated_events_spark(spark_,
                                             ev_heat_table=ev_heat_table,
                                             start_date=start_date,
                                             end_date=end_date,
                                             top_k=top_k)

# COMMAND ----------

display_(heated_events.limit(100))

# COMMAND ----------


def _scrape_from_df_iter(pd_dfs: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    def _scrape_from_df(pd_df: pd.DataFrame) -> pd.DataFrame:
        return pd_df.apply(scr.scrape_one, axis=1)

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
