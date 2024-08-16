from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils

def set_up_date_range_widgets(spark):
    dbutils = DBUtils(spark)
    dbutils.widgets.text("start_date", "", "start_date")
    dbutils.widgets.text("end_date", "", "end_date")
    dbutils.widgets.text("lookback_days", "", "lookback_days")

def get_date_range(spark):
    dbutils = DBUtils(spark)
    start_date = dbutils.widgets.get("start_date")
    end_date = dbutils.widgets.get("end_date")
    lookback_days = dbutils.widgets.get("lookback_days")

    # if we got dates, use them else use current date
    if start_date and end_date:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
    # if we got lookback days, use it
    elif lookback_days:
        start_date = datetime.today() - timedelta(days=int(lookback_days))
        end_date = datetime.today()
    # else use default current date
    else:
        start_date = datetime.today()
        end_date = datetime.today()
    return (start_date.date(), end_date.date())

def set_up_source_widgets(spark):
    dbutils = DBUtils(spark)
    dbutils.widgets.text("source", "", "source")

def get_source(spark):
    dbutils = DBUtils(spark)
    source = dbutils.widgets.get("source")
    return source
















