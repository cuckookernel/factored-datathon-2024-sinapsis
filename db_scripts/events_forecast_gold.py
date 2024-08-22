# Databricks notebook source
!pip install -r ../requirements.txt

# COMMAND ----------

from pyspark.sql import functions as F
import sys
# # instead of export PYTHONPATH='./'
# sys.path.append("/Workspace/Repos/rojas.f.adrian@gmail.com/factored-datathon-2024-sinapsis")
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

silver_events_grouped_df = silver_events_df.groupBy("ev_date", "geo_zone", 
                                                    ).agg(F.mean("heat_indicator").alias("avg_heat_indicator"),
                                                          F.median("action_geo_lat").alias("median_lat"),
                                                          F.median("action_geo_lon").alias("median_lon"))

# COMMAND ----------

# Get distinct dates and countries
dates = silver_events_grouped_df.select("ev_date").where(F.col("ev_date") > "2020-01-01").distinct()
zones = silver_events_grouped_df.select("geo_zone").where(F.col("ev_date") > "2020-01-01").distinct()

# Cross join dates and countries to create a DataFrame with all combinations
complete_combinations = dates.crossJoin(zones)

# Ensure that all combinations are present in the DataFrame
complete_combinations = complete_combinations.withColumn("avg_heat_indicator", F.lit(0))

# COMMAND ----------

final_df = complete_combinations.alias("complete") \
    .join(silver_events_grouped_df.alias("original"), 
          (F.col("complete.ev_date") == F.col("original.ev_date")) & 
          (F.col("complete.geo_zone") == F.col("original.geo_zone")),
          how="left") \
    .select(
        F.col("complete.ev_date"),
        F.col("complete.geo_zone"),
        F.coalesce(F.col("original.avg_heat_indicator"), F.lit(0)).alias("avg_heat_indicator"),
        "median_lat",
        "median_lon"
    )

# COMMAND ----------

display(final_df.orderBy("ev_date").limit(200))

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").partitionBy("ev_date").saveAsTable("gdelt.avg_heat_indicators")


# COMMAND ----------

display(final_df.select(F.col("geo_zone")).distinct())

# COMMAND ----------

!pip install prophet

# COMMAND ----------

from prophet import Prophet
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType
# dataframe needs to have columns ds and y
def forecast_by_group(df):
    df = df.rename(columns={'ev_date': 'ds', 'avg_heat_indicator': 'y'})
    model = Prophet()
    model.fit(df)
    
    # Create future dataframe for predictions
    future = model.make_future_dataframe(periods=7)  # Adjust the periods as needed
    
    # Predict
    forecast = model.predict(future)
    
    # Include group information in the results
    forecast['geo_zone'] = df['geo_zone'] 
    forecast['median_lat'] = df['median_lat']
    forecast['median_lon'] = df['median_lon']
    return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'geo_zone', 'median_lat','median_lon']]

output_schema = StructType([
    StructField("ds", DateType(), True),
    StructField("yhat", FloatType(), True),
    StructField("yhat_lower", FloatType(), True),
    StructField("yhat_upper", FloatType(), True),
    StructField("geo_zone", StringType(), True),
    StructField("median_lat", StringType(), True),
    StructField("median_lon", StringType(), True),
])

# m.plot(forecast)
@F.pandas_udf(output_schema, functionType=F.PandasUDFType.GROUPED_MAP)
def process_udf(pdf)-> pd.DataFrame:
    return forecast_by_group(pdf)

result_df = final_df.groupBy('geo_zone').apply(process_udf)

# COMMAND ----------

display(result_df.limit(20))
