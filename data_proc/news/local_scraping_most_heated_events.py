"""Scraping of news and summarization of articles corresponding to most heated events"""
import os
from pathlib import Path

import pandas as pd

from data_proc.common import gdelt_base_data_path
from data_proc.news.scraping import (
    TBL_SCRAPE_RESULTS,
    TBL_SCRAPE_WISHES,
    get_most_heated_events_pandas,
    get_scraping_db,
    s3_ref_for_url,
    scrape_one,
)
from shared import DataFrame, date, logging, read_env
from shared.s3_utils import get_s3_client

L = logging.getLogger("heated_sum")
# %%


def _interactive_testing(_heated_events: DataFrame) -> None:
    # %%
    from shared import runpyfile
    read_env("/home/teo/profile.sinapsis.env")
    runpyfile("data_proc/news/scraping_most_heated_events.py")
    heat_date = date(2024, 8, 8)
    heated_events = get_most_heated_events_pandas(heat_date=heat_date, top_k=3)
    heated_events_u = heated_events.drop_duplicates(subset=["url_hash"])
    ret: DataFrame = heated_events_u.iloc[:10].apply(scrape_one, axis=1)
    # record = assert_type(heated_events.loc[8], Series)
    # scrape_one(record, use_cache=False)
    # %%
    ret['part_date'] = heat_date
    local_pq_path = _save_to_local_parquet(heat_date, ret)

    _upload_to_s3(heat_date, local_pq_path)
    # %%



def _save_to_local_parquet(heat_date: date, ret: DataFrame) -> Path:
    out_path = gdelt_base_data_path() / 'scraping_results' / f'date_part={heat_date}'
    out_path.mkdir(parents=True, exist_ok=True)
    local_pq_path = out_path / f"{heat_date}.scraping_results.parquet"
    ret.to_parquet(local_pq_path)

    return local_pq_path
    # %%

def _upload_to_s3(heat_date: date, local_pq_path: Path) -> bool:
    client = get_s3_client()
    s3ref = s3_ref_for_url("", success=True)
    s3ref.key = (f"{os.environ['S3_KEY_PREFIX']}/scraping_results/"
                 f"part_date={heat_date}/{local_pq_path.name}")
    return client.put(s3ref , local_pq_path.read_bytes())
    # %%




def _dump_scrape_results_to_parquet() -> None:
    # %%
    from datetime import datetime

    from data_proc.common import UTC, gdelt_base_data_path
    # %%
    now_str = datetime.now(tz=UTC).isoformat()[:-13].replace(":", "")
    db = get_scraping_db()
    results_tbl = db[TBL_SCRAPE_RESULTS]
    results_df = pd.DataFrame(results_tbl.all())
    results_df.to_parquet(gdelt_base_data_path() / f"scrape_results.{now_str}.parquet" )
    db.close()
    # %%
    db = get_scraping_db()
    wishes_tbl = db[TBL_SCRAPE_WISHES]
    wishes_df = pd.DataFrame(wishes_tbl.all())
    wishes_df.to_parquet(gdelt_base_data_path() / f"scrape_wishes.{now_str}.parquet")
    db.close()
    # %%
