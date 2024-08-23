"""Setup widgets controlling the ETL jobs"""
import datetime as dt
from datetime import date, datetime, timedelta

import pytz
from pyspark.dbutils import DBUtils

TZ = pytz.timezone("US/Eastern")

def today() -> date:
    """Tz aware today"""
    return dt.datetime.now(tz=TZ).date()

def set_up_date_range_widgets(spark) -> None:
    dbutils = DBUtils(spark)
    dbutils.widgets.text("start_date", "", "start_date")
    dbutils.widgets.text("end_date", "", "end_date")
    dbutils.widgets.text("lookback_days", "", "lookback_days")

def get_date_range(spark) -> tuple[date, date]:
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
        start_date = (datetime.now(tz=TZ) - timedelta(days=int(lookback_days))).date
        end_date = today()
    # else use default current date
    else:
        start_date = today()
        end_date = today()
    return (start_date, end_date)

def set_up_source_widgets(spark) -> None:
    dbutils = DBUtils(spark)
    dbutils.widgets.text("source", "", "source")

def get_source(spark) -> str:
    dbutils = DBUtils(spark)
    return dbutils.widgets.get("source")

def set_up_force_sync_widgets(spark) -> None:
    dbutils = DBUtils(spark)
    dbutils.widgets.text("force_sync", "", "force_sync")

def get_force_sync(spark) -> bool:
    dbutils = DBUtils(spark)
    force_sync = dbutils.widgets.get("force_sync")
    # default to false if not provided or not valid string
    return force_sync == "true"
