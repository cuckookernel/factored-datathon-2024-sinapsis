# Databricks notebook source
!pip install -r ../requirements.txt

# COMMAND ----------

from pyspark.sql import functions as F
import csv
import sys
# instead of export PYTHONPATH='./'
sys.path.append("/Workspace/Repos/rojas.f.adrian@gmail.com/factored-datathon-2024-sinapsis")
from data_proc.delta_tables_helper import DeltaTableHelper
from shared import logging

logging.getLogger().setLevel(logging.WARNING)
csv.field_size_limit(sys.maxsize)

# COMMAND ----------

# Read the bronze table
bronze_events_schema = DeltaTableHelper.BronzeTables.event_schema

# Assuming there is an "id" column and a way to track transformation, e.g., a "processed" flag or date column
# If silver table already exists, get its IDs to prevent duplicates
if spark.catalog.tableExists("gdelt.silver_events"):
    silver_ids_df = spark.read.table("silver_table").select("ev_id")
else:
    silver_ids_df = spark.createDataFrame([], bronze_events_schema)  # Empty DataFrame if silver does not exist yet

# Get all ev_id in bronze events
bronze_df = spark.read.table(DeltaTableHelper.BronzeTables.event_table).select("ev_id")

# Filter out the already processed rows from the bronze table
bronze_unprocessed_ids_df = bronze_df.join(silver_ids_df, "ev_id", "left_anti")

# COMMAND ----------

# Select the unprocessed rows from the bronze table
bronze_unprocessed_df = spark.read.table(DeltaTableHelper.BronzeTables.event_table).where(F.col("ev_id").isin(bronze_unprocessed_ids_df.select("ev_id")))

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
"act_country_code")

# COMMAND ----------

# Change int to date
bronze_transformed_df = bronze_transformed_df.withColumn("ev_date", F.col("ev_date").cast("date"))
bronze_transformed_df = bronze_transformed_df.withColumn("date_added", F.col("date_added").cast("date"))

# COMMAND ----------

#TODO transformations

# COMMAND ----------

silver_events = DeltaTableHelper.SilverTables.Events

# Now write the transformed data to the silver table
bronze_transformed_df.write.mode("append").partitionBy(silver_events.partition).saveAsTable(silver_events.table_name)
