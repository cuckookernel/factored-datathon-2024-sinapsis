# Databricks notebook source
!pip install -r ../requirements.txt

# COMMAND ----------

from pyspark.sql import functions as F
import sys
# instead of export PYTHONPATH='./'
from data_proc.delta_tables_helper import DeltaTableHelper
from shared import logging

logging.getLogger().setLevel(logging.WARNING)

# COMMAND ----------

from data_proc.widget_helper import set_up_date_range_widgets, get_date_range

set_up_date_range_widgets(spark)

start_date, end_date = get_date_range(spark)
start_date_str, end_date_str = start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")

silver_gkg_counts = DeltaTableHelper.SilverTables.GKGCounts

# COMMAND ----------

# Read the bronze table
bronze_gkg_counts_schema = DeltaTableHelper.BronzeTables.gkg_counts_schema

bronze_df = spark.read.table(DeltaTableHelper.BronzeTables.gkg_counts_table).where(F.col("pub_date").between(start_date_str, end_date_str))

transformed_df = bronze_df.drop("geo_feat_id", "geo_adm1_code")

# COMMAND ----------

# MAGIC %md transform list of strings to array
# MAGIC

# COMMAND ----------

from pyspark.sql.types import ArrayType, IntegerType, StringType

transformed_df = transformed_df.withColumn("event_ids", F.split(transformed_df["event_ids"], ",").cast(ArrayType(IntegerType())))
transformed_df = transformed_df.withColumn("sources", F.split(transformed_df["sources"], ",").cast(ArrayType(StringType())))
transformed_df = transformed_df.withColumn("source_urls", F.split(transformed_df["source_urls"], ",").cast(ArrayType(StringType())))

# COMMAND ----------

# MAGIC %md cast int to date
# MAGIC

# COMMAND ----------

transformed_df = transformed_df.withColumn("pub_date", F.to_date(F.col("pub_date").cast("string"),'yyyyMMdd'))

# COMMAND ----------

# MAGIC %md Joins
# MAGIC

# COMMAND ----------

geo_type_df = spark.read.table("gdelt.geo_type")

geo_df = geo_type_df.select(
    F.col("geo_type").alias("geo_id"),
    F.col("geo_type_desc").alias("geo_type")
)
transformed_df = transformed_df.join(geo_df, on="geo_id", how="left")

# COMMAND ----------

fips_country_codes_df = spark.read.table("gdelt.fips_country_codes")
fips_join_df = fips_country_codes_df.select(
    F.col("fips_country_code").alias("geo_country_code"),
    F.col("country_name").alias("geo_country")
    )

transformed_df = transformed_df.join(fips_join_df, "geo_country_code", "left")

# COMMAND ----------

split_geo_col = F.split(F.col('geo_full_name'), ',')
transformed_df = transformed_df.withColumn("geo_location", F.when(F.size(split_geo_col) == 3, split_geo_col.getItem(0)).otherwise(None)) \
                .withColumn("geo_state", F.when(F.size(split_geo_col) >= 2, split_geo_col.getItem(-2)).otherwise(None))

# COMMAND ----------

transformed_df = transformed_df.drop("geo_full_name")

# COMMAND ----------

transformed_df = transformed_df.select(
    "pub_date",
    "num_articles",
    "count_type",
    "reported_count",
    "object_type",
    "geo_id",
    "geo_type",
    "geo_country_code",
    "geo_country",
    "geo_state",
    "geo_location",
    "geo_lat",
    "geo_lon",
    "event_ids",
    "sources",
    "source_urls",
)

# COMMAND ----------

# Overwrite the silver table with the new data to avoid duplicates if we reprocessed the data
(transformed_df
    .write.mode("overwrite")
    .option("replaceWhere", f"{silver_gkg_counts.partition} >= '{start_date_str}' AND {silver_gkg_counts.partition} <= '{end_date_str}'")
    .partitionBy(silver_gkg_counts.partition)
    .saveAsTable(silver_gkg_counts.table_name))
