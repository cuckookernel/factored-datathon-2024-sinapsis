"""Setup widgets controlling the ETL jobs"""
import datetime as dt
from datetime import date, datetime, timedelta

import pytz
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

TZ = pytz.timezone("US/Eastern")

def today() -> date:
    """Tz aware today"""
    return dt.datetime.now(tz=TZ).date()

def set_up_date_range_widgets(spark: SparkSession) -> None:
    """Set up notebook widgets"""
    dbutils = DBUtils(spark)
    dbutils.widgets.text("start_date", "", "start_date")
    dbutils.widgets.text("end_date", "", "end_date")
    dbutils.widgets.text("lookback_days", "", "lookback_days")

def get_date_range(spark: SparkSession) -> tuple[date, date]:
    """Get the start_date - end_date range"""
    dbutils = DBUtils(spark)
    start_date = dbutils.widgets.get("start_date")
    end_date = dbutils.widgets.get("end_date")
    lookback_days = dbutils.widgets.get("lookback_days")

    # if we got dates, use them else use current date
    if start_date and end_date:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=TZ).date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=TZ).date()
    # if we got lookback days, use it
    elif lookback_days:
        start_date = (datetime.now(tz=TZ) - timedelta(days=int(lookback_days))).date()
        end_date = today()
    # else use default current date
    else:
        start_date = today()
        end_date = today()

    return start_date, end_date

def set_up_source_widgets(spark: SparkSession) -> None:
    """Set up source widget"""
    dbutils = DBUtils(spark)
    dbutils.widgets.text("source", "", "source")

def get_source(spark: SparkSession) -> str:
    """Get value of source widget"""
    dbutils = DBUtils(spark)
    return dbutils.widgets.get("source")

def set_up_force_sync_widgets(spark) -> None:
    """Set up force sync widget/param"""
    dbutils = DBUtils(spark)
    dbutils.widgets.text("force_sync", "", "force_sync")

def get_force_sync(spark) -> bool:
    """Get value of force sync param"""
    dbutils = DBUtils(spark)
    force_sync = dbutils.widgets.get("force_sync")
    # default to false if not provided or not valid string
    return force_sync == "true"


def get_lookback_days(spark: SparkSession) -> int:
    dbutils = DBUtils(spark)
    return int(dbutils.widgets.get("lookback_days"))


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
