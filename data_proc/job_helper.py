"""Setup widgets controlling the ETL jobs"""
from collections.abc import Callable
from datetime import date, datetime, timedelta
from typing import TypeVar

from py4j.protocol import Py4JJavaError
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from data_proc.widget_helper import TZ, today
from shared import assert_type, logging

T_ = TypeVar("T_")

L = logging.getLogger("jh")


def get_param_or_default(
        spark: SparkSession,
        param_name: str,
        default: T_,
        converter: Callable[[str], T_ | None] | None = None,
    ) -> T_:
    """Attempt getting param from a widget and then converting it.

    On failure return default.
    """
    dbutils = DBUtils(spark)
    try:
        param_val_str = dbutils.widgets.get(param_name)
    except Py4JJavaError as err:
        L.warning(f"dbutils.widgets.get({param_name!r}) failed with Py4JJavaError: {err.args[0]}. "
                  f"Returning default={default!r}")
        return default

    if param_val_str == "" or param_val_str is None:
        return default
    else:
        print(f"Got param value from widget or job param  `{param_name}`='{param_val_str}'")
        if converter is not None:
            try:
                return assert_type(converter(param_val_str), type(default))
            except (ValueError, TypeError):
                L.warning(f"Wasn't able to convert param_val_str='{param_val_str}' via "
                          f"converter ({converter}), returning default={default!r}")
                return default
        else:
            return param_val_str

def get_param(
        spark: SparkSession,
        param_name: str,
        converter: Callable[[str], T_] | None = None,
    ) -> T_:
    """Get param or exceptign if not possible"""
    dbutils = DBUtils(spark)

    try:
        param_val_str = dbutils.widgets.get(param_name)
    except Py4JJavaError as err:
        L.warning(f"No widget? {err.args[0]}")
        param_val_str = None

    if param_val_str == "" or param_val_str is None:
       raise RuntimeError(f"No value for param `{param_name}`")

    print(f"Got param value from widget or job param  `{param_name}`='{param_val_str}'")
    if converter is not None:
        return converter(param_val_str)
    else:
        return param_val_str

def get_date_range_from_values(start_date: date | None,
                               end_date: date | None,
                               lookback_days: int | None) -> tuple[date, date]:
    """Get the start_date - end_date range"""
    end_date1: date = end_date if end_date is not None else today()

    # if we got dates, use them else use current date
    if start_date is not None:
        return start_date, end_date1
    # if we got lookback days, use it
    else:
        lookback_days = lookback_days if lookback_days is not None else 1
        start_date1: date = (datetime.now(tz=TZ) - timedelta(days=int(lookback_days))).date()
        return start_date1, end_date1


import pyspark.sql as ss
import pyspark.sql.functions as F


def replace_range(spark: SparkSession,
                  data_sf: ss.DataFrame,
                  part_col: str,
                  dest_table) -> None:

    actual_date_range = data_sf.agg(
        F.min(F.col(part_col)).alias("min_date"),
        F.max(F.col(part_col)).alias("max_date")
    ).collect()

    if len(actual_date_range) == 0:
        print("WARNING: Empty actual date range, doing nothing!")
        return

    actual_date_range_row = actual_date_range[0]

    actual_min_date = actual_date_range_row.min_date
    actual_max_date = actual_date_range_row.max_date
    print(f"ACTUAL DATE RANGE: {actual_min_date} to {actual_max_date}")

    spark.sql(f"""
        delete from {dest_table}
              where {part_col} >= '{actual_min_date}'
                AND {part_col} <= '{actual_max_date}'""")

    (data_sf
     .write
     .mode("append")
     .partitionBy(part_col)
     .saveAsTable(dest_table))
