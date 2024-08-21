"""Scraping and text extraction of news articles from urls found in GDELT datasets"""
import re
from datetime import date
from hashlib import sha256
from http import HTTPStatus
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dataset.database import Database, Table  # type: ignore [import-untyped]
from pandas import Series
from pydantic import BaseModel

from data_proc.common import GdeltV1Type, gdelt_base_data_path
from exploration.utils import sample_data
from shared import logging, runpyfile

L = logging.getLogger('extraction')
# %%

class ScrapeWish(BaseModel):
    """Represents a url we want to eventually scrape"""

    url_hash: str
    url: str
    orig_gdelt_typ: str # Original Gdelt table from which we got this url
    event_id: int | None # only available for events table
    orig_date: date  # for 'events' -> date_added , for 'gkg*' -> pub_date
    num_articles: int
    count_type: str
    geo_country_code: str | None
    geo_full_name: str | None
    metadata: dict[str, object]

class ScrapeResult(BaseModel):
    """Produced after actually scraping"""

    url_hash: str
    status_code: int
    scraped_len: int
    scraped_text_len: int | None
    scraped_text: str | None
    request_err: str | None


REL_DIRS = {
    "events": "last_1y_events",
}

TBL_SCRAPE_WISHES = "scrape_wishes"
TBL_SCRAPE_RESULTS = "scrape_results"
# %%

def gen_scrape_wishlist(typ: GdeltV1Type,
                        start_date: date, end_date: date,
                        fraction: float = 1.0) -> Table:
    """Populate the scrape_wishes list with url records to be scraped later"""
    # %%
    # TODO (Teo?): Generalize this for events as well?
    if typ != 'gkgcounts':
        raise NotImplementedError(f"Not yet implemented for typ=`{typ}`")
    # %%
    db = get_scraping_db()
    db_tbl: Table = db.create_table(TBL_SCRAPE_WISHES,
                                    primary_id="url_hash", primary_type=db.types.string)
    # %%
    db.create_table(TBL_SCRAPE_RESULTS,
                    primary_id="url_hash", primary_type=db.types.string)

    # %%
    # TODO (Adrian): actually get data from databricks...
    gdelt_data = sample_data(typ, rel_dir= Path(f"last_1y_{typ}"),
                             start_date=start_date, end_date=end_date, fraction=fraction)
    gdelt_data['pub_date'] = gdelt_data['pub_date'].astype(str)

    L.info("gdelt_data typ=%s has %d", typ, len(gdelt_data))
    # %%
    wishes = {}
    for _, row in gdelt_data.iterrows():
        for url in set(row['source_urls']):
            metadata=row.to_dict()
            del metadata['pub_date']
            del metadata['sources']
            del metadata['source_urls']
            del metadata['event_ids']

            url_hash = _gen_url_hash(url)
            wish = ScrapeWish(
                url_hash=url_hash,
                url=url,
                orig_gdelt_typ=typ,
                orig_date=row['pub_date'],
                event_id=None,
                metadata=metadata,
                count_type=row['count_type'],
                geo_country_code=row['geo_country_code'],
                geo_full_name=row['geo_full_name'],
                num_articles=row['num_articles'],
            )
            wishes[url_hash] = wish

    L.info("Created %s scrape wishes", len(wishes))
    # %%
    db_tbl.insert_many([wish.dict() for wish in wishes.values()])
    # %%

    return db_tbl
    # %%



def _interactive_testing() -> None:
    # %%
    runpyfile("data_proc/news_extraction/extraction.py")
    # %%
    typ = 'gkgcounts'
    start_date = date(2024, 8, 1)
    end_date = date(2024, 8, 13)
    fraction = 0.1
    print(typ, start_date, end_date, fraction)
    # %%
    # %%
    db = get_scraping_db()
    # %%
    count = pd.DataFrame(db.query("select count(*) from scrape_results"))
    print(count)
    # %%
    db.close()
    # %%

def run_scraping(batch_size: int, limit: int = 1000) -> None:
    """Run scraping and text extraction of yet unscraped urls"""
    # %%
    db = get_scraping_db()

    pending_urls = db.query(f"""
        select w.url_hash, w.url
        from scrape_wishes as w
        left join scrape_results as r
            on  w.url_hash = r.url_hash
        where r.url_hash is null
        order by w.url_hash
        limit {limit}
        """, # noqa: S608
    )
    # %%
    pending_urls_all = pd.DataFrame(pending_urls)
    n_pending = len(pending_urls_all)
    L.info("pending_urls: %d", n_pending)

    batch = []
    n_done = 0
    for _, record in pending_urls_all.iterrows():
        batch.append(record)
        if len(batch) < batch_size:
            continue

        scrape_batch(batch)
        n_done += len(batch)
        L.info(f"Done n_done={n_done} ({n_done/n_pending * 100:.2f} %)")
        batch = []

        scrape_batch(batch)

    db.close()

    # %%



def scrape_batch(batch: list[Series]) -> None:
    """Scrape a whole batch and stored results/errors in the scrape db."""
    results: dict[str, ScrapeResult] = {}

    for record in batch:
        url_hash = record['url_hash']
        L.info('Working on url=%r', record['url'])

        content, status_code, req_err = get_from_cache_or_request(record['url'], url_hash)

        if status_code != HTTPStatus.OK or len(content) == 0:
            results[url_hash] = ScrapeResult(url_hash=url_hash,
                                             status_code=status_code,
                                             scraped_len=0,
                                             scraped_text_len=0,
                                             scraped_text=None,
                                             request_err=req_err)
            continue

        x_text = extract_text(content)
        L.info("url_hash='%s'  x-text-length=%d", url_hash, len(x_text))
        results[url_hash] = ScrapeResult(url_hash=url_hash,
                                         status_code=status_code,
                                         scraped_len=len(content),
                                         scraped_text_len=len(x_text),
                                         scraped_text=x_text,
                                         request_err=None)

    db = get_scraping_db()
    res_tbl = db.get_table(TBL_SCRAPE_RESULTS)
    res_tbl.insert_many([res.dict() for res in results.values()])


def extract_text(content: bytes) -> str:
    """Extract plain human readable text from html code"""
    soup = BeautifulSoup(content, features='html.parser')
    return re.sub("\n+", "\n", soup.text)


def get_from_cache_or_request(url: str,
                              url_hash: str) -> tuple[bytes, int, str|None]:
    """Get (contents, status_code, get request error) for a given url.

    If content is already available is in local cache (filesystem) then get it from there.
    """
    local_path = cache_dir() / f"{url_hash}.dat"
    if local_path.exists():
        return local_path.read_bytes(), HTTPStatus.OK, None
    else:
        try:
            resp = requests.get(url, timeout=30)
        except Exception as err:  # noqa: BLE001
            L.warning(f"request to url='{url}' raised exception={err!r}")
            return (b"", 0, repr(err))

        status_code = resp.status_code
        if status_code == HTTPStatus.OK:
            with local_path.open("wb") as f_out:
                f_out.write(resp.content)
            return resp.content, status_code, None
        else:
            L.warning(f"url='{url}' returned status_code={status_code}")
            return b"", status_code, None
# %%

def cache_dir() -> Path:
    """Get path to scraping cache directory"""
    path = gdelt_base_data_path() / 'scrape_cache'
    path.mkdir(parents=True, exist_ok=True)

    return path


def get_scraping_db() -> Database:
    """Get db handler for scraping"""
    db_path = gdelt_base_data_path() / 'scraping.db'
    return Database(f"sqlite:///{db_path}")


def _gen_url_hash(url: str) -> str:
    return sha256(url.encode("utf8")).hexdigest()[:32]


def dump_scrape_results_to_parquet():
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

if __name__ == "__main__":
    run_scraping(batch_size=10)
