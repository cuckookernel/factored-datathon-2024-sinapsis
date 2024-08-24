"""Setup widgets controlling the ETL jobs"""
from collections.abc import Callable
from datetime import date, datetime, timedelta
from typing import TypeVar

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

from shared import logging
from data_proc.widget_helper import today, TZ
from shared import assert_type

T_ = TypeVar("T_")

L = logging.getLogger("jh")

def get_lookback_days(spark: SparkSession) -> int:
    dbutils = DBUtils(spark)
    return int(dbutils.widgets.get("lookback_days"))


def get_param_or_default(
        spark: SparkSession,
        param_name: str,
        default: T_,
        converter: Callable[[str], T_ | None] | None = None,
    ) -> T_:
    dbutils = DBUtils(spark)
    try:
        param_val_str = dbutils.widgets.get(param_name)
    except Exception as err:
        L.warning(f"dbutils.widgets.get({param_name!r}) failed with error: {err.args[0]}"
                  f"(type:({type(err)}), returning default={default!r}")
        return default

    if param_val_str == "" or param_val_str is None:
        return default
    else:
        print("Got param value from widget or job param  `{param_name}`='{param_val_str}'")
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
    dbutils = DBUtils(spark)
    param_val_str = dbutils.widgets.get(param_name)

    if param_val_str == "" or param_val_str is None:
        raise RuntimeError(f"No value for param `{param_name}")
    else:
        print("Got param value from widget or job param  `{param_name}`='{param_val_str}'")
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
