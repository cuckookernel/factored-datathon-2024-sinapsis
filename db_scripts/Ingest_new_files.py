# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

import csv
import sys
from functools import reduce

import pyspark.sql as ss
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType, StringType, StructType

# instead of export PYTHONPATH='./'
# COMMENT from Teo: changed this to actually point to same repo where this notebook resides
# OLD VERSION:
#   sys.path.append("/Workspace/Repos/rojas.f.adrian@gmail.com/factored-datathon-2024-sinapsis")
sys.path.append("../")
from data_proc.delta_tables_helper import DeltaTableHelper
from data_proc.download import (
    BASE_URL,
    download_and_extract_csv,
    download_file_catalog,
)
from data_proc.widget_helper import (
    get_date_range,
    get_force_sync,
    get_source,
    set_up_date_range_widgets,
    set_up_force_sync_widgets,
    set_up_source_widgets,
)

# from data_proc.widget_helper import *
from shared import logging

logging.getLogger().setLevel(logging.WARNING)
csv.field_size_limit(sys.maxsize)

# COMMAND ----------
# Little hack to enable ruff linting and stop undefined errors from showing in IDE.
# noinspection PyUnboundLocalVariable
spark = spark_ = spark  # noqa: F821, PLW0127
# noinspection PyUnboundLocalVariable
display = display_ = display # noqa: F821, PLW0127

# get widget values
set_up_date_range_widgets(spark)
set_up_source_widgets(spark)
set_up_force_sync_widgets(spark)

start_date, end_date = get_date_range(spark)
start_date_str, end_date_str = start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")

source = get_source(spark)

force_ingestion = get_force_sync(spark)

print({"start_date": start_date, "end_date": end_date, "source": source,
       "force_ingestion": force_ingestion})

# COMMAND ----------

# source configs
if source == "events":
    source_metadata = spark.createDataFrame(download_file_catalog(f"{BASE_URL}/events", "events"))
    source_schema = DeltaTableHelper.BronzeTables.event_schema
    delta_table_name = DeltaTableHelper.BronzeTables.event_table
    delta_table_partition = "date_added"
    header = False
elif source == "gkg":
    source_metadata = spark.createDataFrame(download_file_catalog(f"{BASE_URL}/gkg", "gkg"))
    source_schema = DeltaTableHelper.BronzeTables.gkg_schema
    delta_table_name = DeltaTableHelper.BronzeTables.gkg_table
    delta_table_partition = "pub_date"
    header = True
elif source == "gkgcounts":
    source_metadata = spark.createDataFrame(download_file_catalog(f"{BASE_URL}/gkg", "gkgcounts"))
    source_schema = DeltaTableHelper.BronzeTables.gkg_counts_schema
    delta_table_name = DeltaTableHelper.BronzeTables.gkg_counts_table
    delta_table_partition = "pub_date"
    header = True

else:
    raise ValueError(f"Unknown source: {source}")

# COMMAND ----------

# filter catalog based on date range
filtered_files = source_metadata.filter(F.to_date("date_str", "yyyyMMdd")
                                        .between(start_date, end_date))
display(filtered_files.groupBy("date_str").count())

# COMMAND ----------

# check if any of these files is already ingested
checkpoint_df = (spark.table("gdelt.bronze_scraping_checkpoints")
                 .filter((F.col("event_type") == source )
                     & (F.to_date("date_str", "yyyyMMdd").between(start_date, end_date))))


if force_ingestion:
    files_to_ingest = filtered_files
    new_files = filtered_files.join(checkpoint_df, ["date_str", "event_type"], "left_anti")
else:
    files_to_ingest = filtered_files.join(checkpoint_df, ["date_str", "event_type"], "left_anti")
    new_files = files_to_ingest

print(f"Found {files_to_ingest.count()} new files to ingest.")

# COMMAND ----------

def _csv_to_spark_df(csv_content: str,
                     schema: StructType,
                     header: bool = True,
                     delimiter: str ='\t') -> ss.DataFrame:
    # Create a list of Rows from the CSV content
    csv_reader = csv.reader(csv_content.splitlines(), delimiter=delimiter)

    if header:
        next(csv_reader, None)  # skip the headers

    rows: list[Row] = []
    for row in csv_reader:
        new_row: list[str | int | None | float] = []
        for i, value in enumerate(row):
            field_type = schema[i].dataType
            if isinstance(field_type, IntegerType):
                try:
                    new_row.append(int(value.strip('"')))
                except ValueError:
                    new_row.append(None)
            elif isinstance(field_type, FloatType):
                try:
                    new_row.append(float(value.strip('"')))
                except ValueError:
                    new_row.append(None)
            elif isinstance(field_type, StringType):
                new_row.append(value.strip('"'))
            else:
                new_row.append(value)
        rows.append(Row(*new_row))

    # Convert the list of Rows to a Spark DataFrame
    return spark.createDataFrame(rows, schema)

# COMMAND ----------

#  get list of URLs to download
url_list = files_to_ingest.select("full_url").rdd.flatMap(lambda x: x).collect()
# List to hold the CSV content
response_dfs = []

for url in url_list:
    csv_content = download_and_extract_csv(url)
    if csv_content:
        response_df = _csv_to_spark_df(csv_content, source_schema, header=header, delimiter='\t')
        response_dfs.append(response_df)

# concatenate all the downloaded CSVs into a single DataFrame
if response_dfs:
    union_df = reduce(lambda df1, df2: df1.unionByName(df2), response_dfs)
    display(union_df.groupBy(delta_table_partition).count())

    # insert new data
    (union_df
    .write.mode("overwrite")
    .option("replaceWhere", f"{delta_table_partition} >= '{start_date_str}' "
                            f"AND {delta_table_partition} <= '{end_date_str}'")
    .partitionBy(delta_table_partition)
    .saveAsTable(delta_table_name))

    # update checkpoint table with new files
    (new_files.write.mode("append")
     .partitionBy("event_type")
     .saveAsTable("gdelt.bronze_scraping_checkpoints"))
