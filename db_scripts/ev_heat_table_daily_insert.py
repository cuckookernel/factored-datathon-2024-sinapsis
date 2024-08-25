# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, exp, concat, lit
from data_proc.widget_helper import set_up_date_range_widgets, get_date_range

# COMMAND ----------

set_up_date_range_widgets(spark)

start_date, end_date = get_date_range(spark)
print(f"DATE_RANGE:: {start_date} - {end_date}")

# COMMAND ----------

import math 
ln100 = math.log(100)

# Old filter
# & F.col("ev_root_code").isin(["13", # Threaten
                              # "14", # Protest
                              # "18", # Assault
                              # "19", # Fight
                              # "20"] # Use conventional mass violence)

events_w_heat_sf = (
    spark.read.table("gdelt.silver_events")
    .where(col("date_added")
           .between(start_date, end_date))
    .select("date_added",
            "ev_id",
            "ev_date",     
            "action_geo_country_code", 
            "action_geo_country", 
            "action_geo_state",              
            "action_geo_lat", 
            "action_geo_lon", 
            "is_root_event",
            "gstein_scale",
            "avg_tone",
            "num_sources",
            "num_articles",
            "num_mentions",
            "source_url",
            "quad_class", 
            "quad_class_desc",
            "ev_root_code",
            "ev_root_desc",
            "ev_base_code",
            "ev_code",
            "ev_desc",
    )
    .withColumnRenamed("action_geo_country_code", "country_code")
    .withColumnRenamed("action_geo_country", "country")
    .withColumnRenamed("action_geo_state", "state")
    .withColumnRenamed("action_geo_lat", "lat")
    .withColumnRenamed("action_geo_lon", "lon")
    .withColumnRenamed("ev_date", "indicator_date")
    .withColumn("heat_indicator", 
            # positive values are good (peace / collaboration / ...) => no heat
            # see: https://web.pdx.edu/~kinsella/jgscale.html
            # for negative values we want heat to go exponentially between 0 and 100
            when(col("gstein_scale") >= 0, 0)  
            #  .otherwise( exp(ln100 * (-col("gstein_scale") / 10.0)) - 1 )
            .otherwise ( col("gstein_scale") ** 2 )    
    )
    .filter(col("heat_indicator") > 0)
    .filter(col("country_code").isNotNull() & (col("country_code") != lit("")))
    .withColumn("geo_zone",
                when(col("country_code").isin(["US", "CH", "RS", "IN", "BR"])
                     & col("state").isNotNull() & (col("state") != col("country")),
                     concat(col("country"), lit(" / "), col("state"))
                     )
                .otherwise(col("country"))
    )  # geo_zone
) # events_w_heat_sf

assert len(events_w_heat_sf.columns) == len(set(events_w_heat_sf.columns))


# COMMAND ----------

# events_w_heat_sf.cache().limit(10).display()
spark.sql("DROP TABLE IF EXISTS gdelt.heat_indicator_by_event")

# COMMAND ----------

(events_w_heat_sf
    .write.mode("overwrite")
    .partitionBy("date_added")
    .option("replaceWhere", f"date_added >= '{start_date}' AND date_added <= '{end_date}'")
    .saveAsTable("gdelt.heat_indicator_by_event"))

# COMMAND ----------

from pyspark.sql.functions import log, count

# As this increment may have added events with ev_date (-> inicator date) in the past, 
# we need to recompute for ALL events, not just the increment

heat_by_geo_zone = (
    spark.read.table("gdelt.heat_indicator_by_event")
       .withColumn("log_num_mentions", log(1 + col("num_mentions")))
       .withColumn("weighted_heat", col("heat_indicator") * col("log_num_mentions"))
       .groupBy("indicator_date", "country_code", "state", "geo_zone")
       .agg(
            count(col("ev_id"))             .alias("frequency"),
            F.sum(col("weighted_heat"))     .alias("sum_weighted_heat"),
            F.sum(col("log_num_mentions"))  .alias("sum_log_num_mentions"),
            F.median(col("lat"))            .alias("lat"),
            F.median(col("lon"))            .alias("lon")
        ) # agg
       .withColumn("heat_indicator", 
                   col("sum_weighted_heat") / col("sum_log_num_mentions"))
       .drop("sum_weighted_heat", "sum_log_num_mentions")
)  



# COMMAND ----------

# heat_by_geo_zone.sort("indicator_date", "geo_zone").limit(100).display()

# COMMAND ----------

(heat_by_geo_zone
 .write.mode("overwrite")
 .partitionBy("indicator_date")
 .saveAsTable("gdelt.heat_indicator_by_date_location")
)
