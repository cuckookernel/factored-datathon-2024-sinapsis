# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

import pandas as pd
import logging
from importlib import reload
import data_proc.news.scraping_most_heated_events as scmh

from datetime import date 
from pyspark.sql import DataFrame
from typing import Optional, Iterable
from data_proc.news.scraping import EV_HEAT_TABLE, gen_url_hash
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType
from pyspark.sql.functions import col, udf
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F

logging.getLogger().setLevel("WARN")


# COMMAND ----------

heat_date = date(2023, 8, 11)
top_k = 3

# COMMAND ----------


HEATED_EVENTS_SQL_TMPL = """
    with pre as (
        select
            *,
            row_number() over (partition by geo_zone order by ev_heat desc) as rank            
        from {heat_table}
        where
            heat_date = '{heat_date}'
            and country_code is not null and geo_zone is not null and geo_zone != ''
    )
    select * from pre
        where rank <= {top_k}
""" # noqa: S608

gen_url_hash_udf = udf(gen_url_hash)

def get_most_heated_events_spark(spark: SparkSession, *, heat_date: date, top_k: int) -> DataFrame:
    """Get most top_k most significant events for each geo_zone

    Returns
    -------
        DataFrame with one row per unique url

    """
    query = HEATED_EVENTS_SQL_TMPL.format(heat_table=EV_HEAT_TABLE, heat_date=heat_date, top_k=top_k)
    query_result_df = spark.sql(query).drop_duplicates("source_url")
    ret_df = query_result_df.withColumn('url_hash', 
                                       gen_url_hash_udf(col('source_url')))

    return ret_df

heated_events = get_most_heated_events_spark(spark, heat_date=heat_date, top_k=top_k)


# COMMAND ----------



display(heated_events.limit(100))

# COMMAND ----------


reload(scmh)
def scrape_from_df(pd_df: pd.DataFrame) -> DataFrame:
    return pd_df.apply(scmh.scrape_one, axis=1)

def scrape_from_df_iter(pd_dfs: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    for df in pd_dfs:
        yield scrape_from_df(df)

# COMMAND ----------

results_table = spark.table("gdelt.scraping_results")
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
scrape_results = (heated_events # TODO (MATEO): remove limit
                  .mapInPandas(scrape_from_df_iter, schema=scrape_result_schema)
                  .withColumn('part_date', F.lit(heat_date))
            )

# COMMAND ----------

display(scrape_results.limit(50))

# COMMAND ----------

spark.sql("refresh table gdelt.scraping_results")

# COMMAND ----------

(scrape_results
    .write.mode("overwrite")
    .option("replaceWhere", f"part_date == '{heat_date}'")
    .partitionBy("part_date")
    .saveAsTable("gdelt.scraping_results"))


# COMMAND ----------


