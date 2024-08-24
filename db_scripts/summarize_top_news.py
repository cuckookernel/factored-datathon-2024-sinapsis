# Databricks notebook source
# MAGIC %pip install groq anthropic dataset deflate python-dotenv

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

"""Summarization of selected news via LLMs"""
import logging
from datetime import date
from importlib import reload

import pyspark.sql as ps
from data_proc.news.labeling import GROQ_DEFAULT_MODEL, remove_indentation
import data_proc.news.summarize_news_helpers as snh
from data_proc.job_helper import get_param_or_default, get_date_range_from_values
import data_proc.common as com

reload(com)
reload(snh)

spark = spark_ = spark # noqa: F821, PLW0127
dbutils = dbutils_ = dbutils # noqa: F821, PLW0127

logging.getLogger().setLevel("WARN")


# COMMAND ----------

date_start = get_param_or_default(spark, "start_date", date(2023, 8, 23), com.try_parse_date)
date_end = get_param_or_default(spark, "start_date", date(2023, 8, 23), com.try_parse_date)
lookback_days = get_param_or_default(spark, "lookback_days", 1, int)

date_start, date_end = get_date_range_from_values(date_start, date_end, lookback_days)


# COMMAND ----------

query_text = f"""select *
            from gdelt.scraping_results
            where scraped_text is not null
            and part_date >= '{date_start}'
            and part_date <= '{date_end}'
            """ # noqa: S608
print(query_text)
scraped_new_sf: ps.DataFrame = spark.sql(query_text)
scraped_news_sf = scraped_new_sf.repartition(numPartitions=snh.n_grok_api_keys()).cache()

scraped_news_sf.limit(30).display()

# COMMAND ----------

print(f"SCRAPED NEWS COUNT {date_start} - {date_end} ::", scraped_news_sf.count())

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

summaries_df = spark.createDataFrame(summaries_rdd, schema=snh.SUMMARIZE_RESULT_SCHEMA)
summaries_df.cache().limit(10).display()

# COMMAND ----------

(summaries_df
    .write.mode("overwrite")
    .option("replaceWhere", f"part_date >= '{date_start}' and part_date <= '{date_end}'")
    .partitionBy("part_date")
    .saveAsTable("gdelt.summary_results"))


# COMMAND ----------
