# Databricks notebook source
# MAGIC %pip install groq anthropic dataset deflate python-dotenv

# COMMAND ----------

"""Summarization of selected news via LLMs"""
import logging
import sys
from datetime import date
from importlib import reload

import pyspark.sql.functions as F
import pyspark.sql as ps

import data_proc.common as com
import data_proc.news.summarize_news_helpers as snh
from data_proc.widget_helper import get_date_range, set_up_date_range_widgets
from data_proc.news.labeling import GROQ_DEFAULT_MODEL

reload(com)
reload(snh)

spark = spark_ = spark # noqa: F821, PLW0127
dbutils = dbutils_ = dbutils # noqa: F821, PLW0127

logging.getLogger().setLevel("WARN")
print("PYTHON VERSION ::", sys.version)
set_up_date_range_widgets(spark)

# COMMAND ----------


start_date, end_date = get_date_range(spark)
print(f"DATE RANGE: {start_date} ==> {end_date}")

# COMMAND ----------

query_text = f"""
            select *
            from 
                gdelt.scraping_results
            where scraped_text is not null
                and part_date >= '{start_date}'
                and part_date <= '{end_date}'
            """ # noqa: S608
print(query_text)
scraped_news_sf = (
    spark.sql(query_text)
    .repartition(numPartitions=snh.n_grok_api_keys())
).cache()


# COMMAND ----------

(scraped_news_sf
    .groupby("part_date")
    .agg(F.count("scraped_text"))
    .orderBy("part_date", ascending=False)
).show()
print(f"SCRAPED NEWS COUNT {start_date} - {end_date} ::", scraped_news_sf.count())

# COMMAND ----------

partition_row_iter_mapper = snh.make_partition_with_index_mapper(
          id_col="url_hash",
          news_text_col="scraped_text",
          part_date_col="part_date",
          groq_model=GROQ_DEFAULT_MODEL,
          prompt_tmpl=snh.SUMMARIZE_LLM_TMPL,
          llm_req_params={"max_tokens": 1024,
                          "input_trunc_len": 2000}
)

summaries_rdd = (scraped_news_sf
                 .rdd.mapPartitionsWithIndex(partition_row_iter_mapper)
                ).cache()

# COMMAND ----------

summaries_df = spark.createDataFrame(summaries_rdd, 
                                     schema=snh.SUMMARIZE_RESULT_SCHEMA)
(summaries_df
    .cache()
    .groupby("part_date")
    .agg(F.count("summary"))
 ).show()

# COMMAND ----------

dest_table = "gdelt.summary_results"

import pyspark.sql as ss;
from pyspark.sql.session import SparkSession

def replace_range(spark: SparkSession,
                  data_sf: ss.DataFrame,
                  part_col: str,
                  dest_table) -> None:

    actual_date_range = data_sf.agg(
        F.min(F.col(part_col)).alias("min_date"),
        F.max(F.col(part_col)).alias("max_date")
    ).collect()

    if len(actual_date_range) == 0:
        print("WARNING: Empty actual date range, doing nothing!")
        return

    actual_date_range_row = actual_date_range[0]

    actual_min_date = actual_date_range_row.min_date
    actual_max_date = actual_date_range_row.max_date
    print(f"ACTUAL DATE RANGE: {actual_min_date} to {actual_max_date}")

    spark.sql(f"""
        delete from {dest_table}
              where {part_col} >= '{actual_min_date}'
                AND {part_col} <= '{actual_max_date}'""")

    (data_sf
     .write
     .mode("append")
     .partitionBy(part_col)
     .saveAsTable(dest_table))


replace_range(spark, summaries_df, "part_date", dest_table)


# COMMAND ----------


