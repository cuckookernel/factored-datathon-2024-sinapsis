# Databricks notebook source
!pip install -r ../requirements.txt

# COMMAND ----------

from pyspark.sql import functions as F
import sys
from data_proc.delta_tables_helper import DeltaTableHelper
from shared import logging

logging.getLogger().setLevel(logging.WARNING)

# COMMAND ----------

from data_proc.widget_helper import set_up_date_range_widgets, get_date_range

set_up_date_range_widgets(spark)

start_date, end_date = get_date_range(spark)
start_date_str, end_date_str = start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")

# COMMAND ----------

training_data_start_date = end_date - F.expr("INTERVAL 3 MONTHS")

heat_geo_df = spark.read.table("gdelt.heat_indicator_by_date_location").where(F.col("indicator_date") > training_data_start_date)

# COMMAND ----------

# Get distinct dates and countries
dates = heat_geo_df.select("indicator_date").distinct()
zones = heat_geo_df.select("geo_zone", "country_code", "country").distinct()

# Cross join dates and countries to create a DataFrame with all combinations
complete_combinations = dates.crossJoin(zones)

# COMMAND ----------

final_df = complete_combinations.alias("complete") \
    .join(heat_geo_df.alias("original"), 
          (F.col("complete.indicator_date") == F.col("original.indicator_date")) & 
          (F.col("complete.geo_zone") == F.col("original.geo_zone")),
          how="left") \
    .select(
        F.col("complete.indicator_date"),
        F.col("complete.geo_zone"),
         F.col("complete.country_code"),
        F.col("complete.country"),
        F.coalesce(F.col("original.heat_indicator"), F.lit(0)).alias("heat_indicator"),
        F.coalesce(F.col("original.frequency"), F.lit(0)).alias("frequency"),
    )

# COMMAND ----------

final_df.select('country_code').distinct().count()

# COMMAND ----------

X_df = final_df.groupBy("indicator_date", "country_code", "country").agg(F.mean("heat_indicator").alias("heat_indicator"))

# COMMAND ----------

!pip install diviner

# COMMAND ----------

from diviner import GroupedProphet

X_df = X_df.withColumnRenamed("indicator_date", "ds").withColumnRenamed("heat_indicator", "y")
model = GroupedProphet().fit(X_df.toPandas(), group_key_columns=["country_code"]);

# COMMAND ----------

predictions = model.forecast(horizon=7, frequency="D")

# COMMAND ----------

spark_predictions_df = spark.createDataFrame(predictions)

# COMMAND ----------

final_predictions_df = zones.alias("complete") \
    .join(spark_predictions_df.alias("original"), 
          (F.col("complete.country_code") == F.col("original.country_code")),
          how="inner") \
    .select(
        F.col("original.ds").alias("indicator_date"),
        F.col("original.country_code"),
        F.col("complete.country"),
        F.col('original.yhat'),
        F.col('original.yhat_upper'),
        F.col('original.yhat_lower'),
        F.lit(end_date).alias('date_predicted')
    )

# COMMAND ----------

final_predictions_df = final_predictions_df.withColumn("indicator_date", F.col("indicator_date").cast("date"))

# COMMAND ----------

(final_predictions_df
    .write.mode("overwrite")
.option("replaceWhere", f"date_predicted = '{end_date}'")
    .partitionBy("date_predicted")
    .saveAsTable("gdelt.heat_indicator_by_date_location_forecast_7d"))
