# Databricks notebook source
!pip install -r ../requirements.txt

# COMMAND ----------

from pyspark.sql import functions as F
import sys
# instead of export PYTHONPATH='./'
sys.path.append("/Workspace/Repos/rojas.f.adrian@gmail.com/factored-datathon-2024-sinapsis")
from data_proc.delta_tables_helper import DeltaTableHelper
from shared import logging

logging.getLogger().setLevel(logging.WARNING)

# COMMAND ----------

from data_proc.widget_helper import set_up_date_range_widgets, get_date_range

set_up_date_range_widgets(spark)

start_date, end_date = get_date_range(spark)
start_date_str, end_date_str = start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")

silver_events = DeltaTableHelper.SilverTables.Events

# COMMAND ----------

# Read the bronze table
bronze_events_schema = DeltaTableHelper.BronzeTables.event_schema

bronze_df = spark.read.table(DeltaTableHelper.BronzeTables.event_table).where(F.col("date_added").between(start_date_str, end_date_str))

# If silver table already exists, get its IDs to prevent duplicates
if spark.catalog.tableExists(silver_events.table_name):
    silver_ids_df = spark.read.table(silver_events.table_name).select("ev_id").where(F.col("date_added").between(start_date, end_date))
else:
    silver_ids_df = spark.createDataFrame([], bronze_events_schema)  # Empty DataFrame if silver does not exist yet

# Filter out the already processed rows from the bronze table
bronze_unprocessed_df = bronze_df.join(silver_ids_df, "ev_id", "left_anti")

# COMMAND ----------

# Drop the unnecessary columns
bronze_transformed_df = bronze_unprocessed_df.drop("year_month",
"year",
"date_fraction",
"a1_code",
"a1_group_code",
"a1_ethnic_code",
"a1_rel1_code",
"a1_rel2_code",
"a1_type3_code",
"a2_code",
"a2_group_code",
"a2_ethnic_code",
"a2_rel1_code",
"a2_rel2_code",
"a2_type3_code",
"a1_geo_country_code",
"a2_geo_country_code",
"action_geo_country_code",
"a1_geo_feat_id",
"a2_geo_feat_id",
"action_geo_feat_id",
"a1_geo_adm1_code",
"a2_adm1_code",
"action_geo_adm1_code")

# COMMAND ----------

# MAGIC %md Geo Split
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, split, when, size


# Split the 'full_geo' column into segments
a1_split_geo_col = split(col('a1_geo_full_name'), ',')
bronze_transformed_df = bronze_transformed_df.withColumn("a1_geo_location", when(size(a1_split_geo_col) == 3, a1_split_geo_col.getItem(0)).otherwise(None)) \
                .withColumn("a1_geo_state", when(size(a1_split_geo_col) >= 2, a1_split_geo_col.getItem(-2)).otherwise(None)) \
                .withColumn("a1_geo_country", a1_split_geo_col.getItem(-1))


a2_split_geo_col = split(col('a2_geo_full_name'), ',')
bronze_transformed_df = bronze_transformed_df.withColumn("a2_geo_location", when(size(a2_split_geo_col) == 3, a2_split_geo_col.getItem(0)).otherwise(None)) \
                .withColumn("a2_geo_state", when(size(a2_split_geo_col) >= 2, a2_split_geo_col.getItem(-2)).otherwise(None)) \
                .withColumn("a2_geo_country", a2_split_geo_col.getItem(-1))


action_split_geo_col = split(col('action_geo_full_name'), ',')
bronze_transformed_df = bronze_transformed_df.withColumn("action_geo_location", when(size(action_split_geo_col) == 3, action_split_geo_col.getItem(0)).otherwise(None)) \
                .withColumn("action_geo_state", when(size(action_split_geo_col) >= 2, action_split_geo_col.getItem(-2)).otherwise(None)) \
                .withColumn("action_geo_country", action_split_geo_col.getItem(-1))

# COMMAND ----------

# MAGIC %md Data type transform
# MAGIC

# COMMAND ----------

# Change int to date
bronze_transformed_df = bronze_transformed_df.withColumn("ev_date", F.to_date(F.col("ev_date").cast("string"),'yyyyMMdd'))
bronze_transformed_df = bronze_transformed_df.withColumn("date_added", F.to_date(F.col("date_added").cast("string"),'yyyyMMdd'))

# COMMAND ----------

# MAGIC %md Join event codes
# MAGIC

# COMMAND ----------

cameo_codes_df = spark.read.table("gdelt.cameo_ev_codes")

ev_root_df = cameo_codes_df.select(
  F.col("ev_code").alias("ev_root_code"),
  F.col("ev_desc").alias("ev_root_desc")
)
ev_base_df = cameo_codes_df.select(
  F.col("ev_code").alias("ev_base_code"),
  F.col("ev_desc").alias("ev_base_desc")
  )
ev_df = cameo_codes_df

bronze_transformed_df = bronze_transformed_df.join(ev_root_df, "ev_root_code", "left")
bronze_transformed_df = bronze_transformed_df.join(ev_base_df, "ev_base_code", "left")
bronze_transformed_df = bronze_transformed_df.join(cameo_codes_df, "ev_code", "left")

# COMMAND ----------

quad_classes_df = spark.read.table("gdelt.gdelt_quad_classes")
bronze_transformed_df = bronze_transformed_df.join(quad_classes_df.select("quad_class", "ev_desc").withColumnRenamed("ev_desc", "quad_class_desc"), "quad_class", "left")

# COMMAND ----------

# MAGIC %md Join type codes
# MAGIC

# COMMAND ----------

cameo_actor_type_df = spark.read.table("gdelt.cameo_actor_type")

a1_t1 = cameo_actor_type_df.select(
    F.col("a_type_code").alias("a1_type1_code"),
    F.col("a_type_desc").alias("a1_type1_desc")
)
a1_t2 = cameo_actor_type_df.select(
    F.col("a_type_code").alias("a1_type2_code"),
    F.col("a_type_desc").alias("a1_type2_desc")
)
a2_t1 = cameo_actor_type_df.select(
    F.col("a_type_code").alias("a2_type1_code"),
    F.col("a_type_desc").alias("a2_type1_desc")
)
a2_t2 = cameo_actor_type_df.select(
    F.col("a_type_code").alias("a2_type2_code"),
    F.col("a_type_desc").alias("a2_type2_desc")
)

bronze_transformed_df = bronze_transformed_df.join(a1_t1, on="a1_type1_code", how="left")
bronze_transformed_df = bronze_transformed_df.join(a1_t2, on="a1_type2_code", how="left")
bronze_transformed_df = bronze_transformed_df.join(a2_t1, on="a2_type1_code", how="left")
bronze_transformed_df = bronze_transformed_df.join(a2_t2, on="a2_type2_code", how="left")

# COMMAND ----------

# MAGIC %md Join Geo Type
# MAGIC

# COMMAND ----------

geo_type_df = spark.read.table("gdelt.geo_type")

a1_geo_df = geo_type_df.select(
    F.col("geo_type").alias("a1_geo_id"),
    F.col("geo_type_desc").alias("a1_geo_type")
)
a2_geo_df = geo_type_df.select(
    F.col("geo_type").alias("a2_geo_id"),
    F.col("geo_type_desc").alias("a2_geo_type")
    )
action_geo_df = geo_type_df.select(
    F.col("geo_type").alias("action_geo_id"),
    F.col("geo_type_desc").alias("action_geo_type")
    )
bronze_transformed_df = bronze_transformed_df.join(a1_geo_df, on="a1_geo_id", how="left")
bronze_transformed_df = bronze_transformed_df.join(a2_geo_df, on="a2_geo_id", how="left")
bronze_transformed_df = bronze_transformed_df.join(action_geo_df, on="action_geo_id", how="left")

# COMMAND ----------

# MAGIC %md Join actor country
# MAGIC

# COMMAND ----------

cameo_country_df = spark.read.table("gdelt.cameo_country")

a1_country_df = cameo_country_df.select(
  "cameo_country_code",
  F.col("country_name").alias("a1_country_name")
  )
a2_country_df = cameo_country_df.select(
  "cameo_country_code",
  F.col("country_name").alias("a2_country_name")
  )

bronze_transformed_df = bronze_transformed_df.join(a1_country_df, on=bronze_transformed_df.a1_country_code == a1_country_df.cameo_country_code, how="left")
bronze_transformed_df = bronze_transformed_df.join(a2_country_df, on=bronze_transformed_df.a2_country_code == a2_country_df.cameo_country_code, how="left")

# COMMAND ----------

bronze_transformed_df.limit(10).display()

# COMMAND ----------

bronze_transformed_df.columns

# COMMAND ----------

bronze_transformed_df = bronze_transformed_df.drop("cameo_country_code", "_rescued_data", "a1_geo_full_name", "a2_geo_full_name", "action_geo_full_name")

bronze_transformed_df = bronze_transformed_df.select(
    "ev_id",
    "ev_date",

    "a1_name",
    "a1_country_code",
    "a1_country_name",
    "a1_type1_code",
    "a1_type1_desc",
    "a1_type2_code",
    "a1_type2_desc",
    "a1_geo_id",
    "a1_geo_type",
    "a1_geo_country",
    "a1_geo_state",
    "a1_geo_location",
    "a1_geo_lat",
    "a1_geo_lon",

    "a2_name",
    "a2_country_code",
    "a2_country_name",
    "a2_type1_code",
    "a2_type1_desc",
    "a2_type2_code",
    "a2_type2_desc",
    "a2_geo_id",
    "a2_geo_type",
    "a2_geo_country",
    "a2_geo_state",
    "a2_geo_location",
    "a2_geo_lat",
    "a2_geo_lon",

    "action_geo_country",
    "action_geo_state",
    "action_geo_location",
    "action_geo_id",
    "action_geo_type",
    "action_geo_lat",
    "action_geo_lon",

    "is_root_event",
    "ev_code",
    "ev_desc",
    "ev_base_code",
    "ev_base_desc",
    "ev_root_code",
    "ev_root_desc",
    "quad_class",
    "quad_class_desc",
    "gstein_scale",
    "num_mentions",
    "num_sources",
    "num_articles",
    "avg_tone",

    "date_added",
    "source_url"
)

# COMMAND ----------

# Now write the transformed data to the silver table
# bronze_transformed_df.write.mode("append").partitionBy(silver_events.partition).saveAsTable(silver_events.table_name)

# Overwrite the silver table with the new data to avoid duplicates if we reprocessed the data
(bronze_transformed_df
    .write.mode("overwrite")
    .option("replaceWhere", f"{silver_events.partition} >= '{start_date_str}' AND {silver_events.partition} <= '{end_date_str}'")
    .partitionBy(silver_events.partition)
    .saveAsTable(silver_events.table_name))
