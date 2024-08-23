# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

import pandas as pd
from data_proc.news.scraping_most_heated_events import scrape_one

from datetime import date 
from pyspark.sql import DataFrame
from typing import Optional, Iterable
from data_proc.news.scraping import EV_HEAT_TABLE, gen_url_hash
from pyspark.sql.functions import col, udf
from pyspark.sql.session import SparkSession


# COMMAND ----------

heat_date = '2023-08-12'
top_k = 1

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
        DataFrame with one row per event

    """
    query = HEATED_EVENTS_SQL_TMPL.format(heat_table=EV_HEAT_TABLE, heat_date=heat_date, top_k=top_k)
    
    ret_df = spark.sql(query).withColumn('url_hash', 
                                         gen_url_hash_udf(col('source_url'))
                                         )

    return ret_df

heated_events = get_most_heated_events_spark(spark, heat_date=heat_date, top_k=top_k)


# COMMAND ----------

display(heated_events.limit(100))

# COMMAND ----------

def scrape_from_df(pd_df: pd.DataFrame) -> DataFrame:
    return pd_df.apply(scrape_one, axis=1)

def scrape_from_df_iter(pd_dfs: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    for df in pd_dfs:
        yield scrape_from_df(df)

# COMMAND ----------

results_table = spark.table("gdelt.scraping_results")
scrape_results = heated_events.mapInPandas(scrape_from_df_iter, schema=results_table.schema)

# COMMAND ----------

display(scrape_results.limit(50))

# COMMAND ----------


