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


silver_events = DeltaTableHelper.SilverTables.Events

# COMMAND ----------

silver_events_df = spark.read.table(silver_events.table_name).where(
    F.col("date_added").between(start_date, end_date)
    & F.col("ev_root_code").isin(["13","14","18","20"])
    # & (F.col("is_root_event") == True)
    ).select("ev_id","ev_date", 
                  "action_geo_country_code", "action_geo_country", "action_geo_state", "action_geo_location", "action_geo_id", "action_geo_type", "action_geo_lat", "action_geo_lon", "is_root_event", "ev_code","ev_desc","ev_base_code","ev_base_desc","ev_root_code","ev_root_desc","quad_class","quad_class_desc","gstein_scale","num_mentions","num_sources","num_articles","avg_tone","date_added","source_url")

# COMMAND ----------

silver_events_df.count()

# COMMAND ----------

silver_events_df = silver_events_df.withColumn("heat_indicator", 
                                               ((F.col("gstein_scale") + F.col("avg_tone")) / 2) * F.col("num_mentions")
                                               )

# COMMAND ----------

display(silver_events_df.select("action_geo_country_code", "action_geo_country").distinct().orderBy("action_geo_country"))

# COMMAND ----------

silver_events_df = silver_events_df.withColumn("geo_zone", F.when(F.col("action_geo_country_code").isin(["US", "CH", "BR", "IN", "IS", "RS", "UK"]), F.col("action_geo_state")).otherwise(F.col("action_geo_country")))

# COMMAND ----------

silver_events_grouped_df = silver_events_df.groupBy("ev_date", "geo_zone", F.col('action_geo_country').alias('country')
                                                    ).agg(F.mean("heat_indicator").alias("heat_indicator"),
                                                          F.median("action_geo_lat").alias("lat"),
                                                          F.median("action_geo_lon").alias("lon"),
                                                          F.count('*').alias("frequency")
                                                          )

# COMMAND ----------

display(silver_events_grouped_df.limit(100))

# COMMAND ----------

silver_events_grouped_df = silver_events_grouped_df.filter(silver_events_grouped_df.geo_zone.isNotNull())

# COMMAND ----------

lat_lon_df = silver_events_grouped_df.groupBy("geo_zone").agg(F.median('lat').alias('lat'), F.median('lon').alias('lon'))

# COMMAND ----------

# Get distinct dates and countries
dates = silver_events_grouped_df.select("ev_date").where(F.col("ev_date") > "2020-01-01").distinct()
zones = silver_events_grouped_df.select("geo_zone", "country").where(F.col("ev_date") > "2020-01-01").distinct()

# Cross join dates and countries to create a DataFrame with all combinations
complete_combinations = dates.crossJoin(zones).join(lat_lon_df, "geo_zone")

# Ensure that all combinations are present in the DataFrame
complete_combinations = complete_combinations.withColumn("heat_indicator", F.lit(0))

# COMMAND ----------

complete_combinations.count()

# COMMAND ----------

final_df = complete_combinations.alias("complete") \
    .join(silver_events_grouped_df.alias("original"), 
          (F.col("complete.ev_date") == F.col("original.ev_date")) & 
          (F.col("complete.geo_zone") == F.col("original.geo_zone")),
          how="left") \
    .select(
        F.col("complete.ev_date").alias("indicator_date"),
        F.col("complete.geo_zone"),
        F.col("complete.country"),
        F.coalesce(F.col("original.heat_indicator"), F.lit(0)).alias("heat_indicator"),
        F.coalesce(F.col("original.frequency"), F.lit(0)).alias("frequency"),
        F.col("complete.lat"),
        F.col("complete.lon")
    )

# COMMAND ----------

display(final_df.orderBy("indicator_date").limit(200))

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").partitionBy("indicator_date").saveAsTable("gdelt.heat_indicator_by_date_location")

# COMMAND ----------

display(final_df.select(F.col("geo_zone")).distinct())

# COMMAND ----------

!pip install diviner

# COMMAND ----------

from diviner import GroupedProphet

fit_df = final_df.withColumnRenamed("indicator_date", "ds").withColumnRenamed("heat_indicator", "y")
model = GroupedProphet().fit(fit_df.toPandas(), group_key_columns=["geo_zone"])

# COMMAND ----------

predictions = model.forecast(horizon=1, frequency="W")

# COMMAND ----------

print(predictions.columns)

# COMMAND ----------

print(predictions['yhat'])

# COMMAND ----------

spark_predictions_df = spark.createDataFrame(predictions)

# COMMAND ----------

display(spark_predictions_df.limit(10))

# COMMAND ----------

spark_predictions_df.count()

# COMMAND ----------

geo_combinations = zones.join(lat_lon_df, "geo_zone")

final_predictions_df = geo_combinations.alias("complete") \
    .join(spark_predictions_df.alias("original"), 
          (F.col("complete.geo_zone") == F.col("original.geo_zone")),
          how="inner") \
    .select(
        F.col("original.ds").alias("indicator_date"),
        F.col("original.geo_zone"),
        F.col("complete.country"),
        F.col('original.yhat'),
        F.col('original.yhat_upper'),
        F.col('original.yhat_lower'),
        F.col("complete.lat"),
        F.col("complete.lon"),
    )

# COMMAND ----------

display(final_predictions_df.limit(20))

# COMMAND ----------

final_predictions_df = final_predictions_df.withColumn("indicator_date", F.col("indicator_date").cast("date"))

# COMMAND ----------

(final_predictions_df
    .write.mode("overwrite")
    .partitionBy("indicator_date")
    .saveAsTable("gdelt.heat_indicator_by_date_location_forecast_7d"))
