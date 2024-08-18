# Databricks notebook source
!pip install -r ../requirements.txt

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import sys

# instead of export PYTHONPATH='./'
sys.path.append("/Workspace/Repos/rojas.f.adrian@gmail.com/factored-datathon-2024-sinapsis")
from data_proc.widget_helper import *
from data_proc.delta_tables_helper import DeltaTableHelper
from data_proc.common import GdeltV1Type
from shared import logging

logging.getLogger().setLevel(logging.WARNING)

# COMMAND ----------

# get widget values
set_up_date_range_widgets(spark)

start_date, end_date = get_date_range(spark)
start_date_str, end_date_str = start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")

start_date_str, end_date_str

# COMMAND ----------

gkg_table = spark.read.table(DeltaTableHelper.BronzeTables.gkg_table).filter(F.to_date("pub_date", "yyyyMMdd").between(start_date, end_date))
display(gkg_table.groupBy('pub_date').count())

# COMMAND ----------

def split_column_to_list(column_name, split_char):
    cleaned_column = F.regexp_replace(F.col(column_name), f"{split_char}$", "")  # Remove trailing split_char
    
    # Return None if the string is empty or only spaces
    validated_column = F.when(cleaned_column == "", None).otherwise(cleaned_column)

    list_column = F.split(validated_column, split_char)
    return list_column

# COMMAND ----------

gkg_transformed = (gkg_table
                   .withColumn("themes", split_column_to_list("themes", ";"))
                   .withColumn("persons", split_column_to_list("persons", ";"))
                   .withColumn("organizations", split_column_to_list("organizations", ";"))
                   .withColumn("event_ids", split_column_to_list("event_ids", ","))
                   .withColumn("sources", split_column_to_list("sources", ";"))
                   .withColumn("source_urls", split_column_to_list("source_urls", "<UDIV>"))
                   .withColumn("counts", DeltaTableHelper.SilverTables.GKG.parse_gkg_count_entry_udf("counts"))
                   .withColumn("locations", DeltaTableHelper.SilverTables.GKG.parse_gkg_location_entry_udf("locations"))
                   .withColumn("tone_vec", DeltaTableHelper.SilverTables.GKG.parse_tone_udf("tone_vec"))
                   )

# COMMAND ----------

(gkg_transformed
    .write.mode("overwrite")
    .option("replaceWhere", f"{DeltaTableHelper.SilverTables.GKG.partition} >= '{start_date_str}' AND {DeltaTableHelper.SilverTables.GKG.partition} <= '{end_date_str}'")
    .partitionBy(DeltaTableHelper.SilverTables.GKG.partition)
    .saveAsTable(DeltaTableHelper.SilverTables.GKG.table_name))
