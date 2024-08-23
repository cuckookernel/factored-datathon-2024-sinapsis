"""Scraping and text extraction of news articles from urls found in GDELT datasets"""
import json
import os
import re
from base64 import b85decode, b85encode
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from http import HTTPStatus
from pathlib import Path
from pprint import pformat
from typing import Self, TypeAlias

import brotli  # type: ignore [import-untyped]
import deflate  # type: ignore [import-untyped]
import pandas as pd
import pyspark.sql as ps
import requests
from bs4 import BeautifulSoup
from dataset.database import Database, Table  # type: ignore [import-untyped]
from pandas import Series
from pydantic import BaseModel
from pyspark.sql.functions import col, udf
from pyspark.sql.session import SparkSession

from data_proc.common import GdeltV1Type, gdelt_base_data_path
from shared import assert_type, assert_type_or_none, logging, runpyfile
from shared.s3_utils import S3Client, S3Ref, get_s3_client

REQ_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    # " (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept": "text/html,application/xhtml+xml",
    "Referer": "http://www.google.com",
}
# from shared.databricks_conn import run_query

L = logging.getLogger('extraction')

EV_HEAT_TABLE = "gdelt.heat_indicator_by_event_dummy_teo"
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
    source_url: str
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
                        start_date: date,
                        # end_date: date,
                        _fraction: float = 1.0) -> Table:
    """Populate the scrape_wishes list with url records to be scraped later"""
    # %%
    if typ != 'gkgcounts':
        raise NotImplementedError(f"Not yet implemented for typ=`{typ}`")
    # %%
    db = get_scraping_db()
    db_tbl: Table = db.create_table(TBL_SCRAPE_WISHES,
                                    primary_id="url_hash", primary_type=db.types.string)
    db.create_table(TBL_SCRAPE_RESULTS,
                    primary_id="url_hash", primary_type=db.types.string)


    # gdelt_data = sample_data(typ, rel_dir= Path(f"last_1y_{typ}"),
    #                          start_date=start_date, end_date=end_date, fraction=fraction)
    gdelt_data = get_most_heated_events_pandas(heat_date = start_date, top_k = 1)
    gdelt_data['pub_date'] = gdelt_data['heat_date'].astype(str)

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

            url_hash = gen_url_hash(url)
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
    db_tbl.insert_many([wish.dict() for wish in wishes.values()])

    return db_tbl
# %%


HEATED_EVENTS_SQL_TMPL = """
    with pre as (
        select
            *,
            row_number() over (partition by geo_zone order by ev_heat desc) as rank
        from {heat_table}
        where
            heat_date = '{heat_date}'
            and country_code is not null and geo_zone is not null and geo_zone != ''
    )
    select * from pre
        where rank <= {top_k}
"""


def get_most_heated_events_pandas(*, heat_date: date, top_k: int) -> pd.DataFrame:
    """Get most top_k most significant events for each geo_zone

    Returns
    -------
        DataFrame with one row per event

    """
    query_sql = HEATED_EVENTS_SQL_TMPL.format(heat_table=EV_HEAT_TABLE,
                                              heat_date=heat_date,
                                              top_k=top_k)

    from shared.databricks_conn import run_query
    ret_df = run_query(query_sql)
    ret_df['url_hash'] = ret_df['source_url'].apply(gen_url_hash)
    ret_df['heat_date'] = heat_date

    return ret_df



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
        L.info(f"Done n_done={n_done} ({n_done / n_pending * 100:.2f} %)")
        batch = []

        scrape_batch(batch)

    db.close()

    # %%
def get_most_heated_events_spark(spark: SparkSession, *,
                                 heat_date: date, top_k: int) -> ps.DataFrame:
    """Get most top_k most significant events for each geo_zone

    Returns
    -------
        DataFrame with one row per unique url

    """
    query = HEATED_EVENTS_SQL_TMPL.format(heat_table=EV_HEAT_TABLE, heat_date=heat_date,
                                          top_k=top_k)
    query_result_df = spark.sql(query).drop_duplicates(["source_url"])
    gen_url_hash_udf = udf(gen_url_hash)
    return query_result_df.withColumn('url_hash', gen_url_hash_udf(col('source_url')))


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
                                             request_err=req_err,
                                             source_url=record['url'])
            continue

        x_text = extract_text(content)
        L.info("url_hash='%s'  x-text-length=%d", url_hash, len(x_text))
        results[url_hash] = ScrapeResult(url_hash=url_hash,
                                         status_code=status_code,
                                         scraped_len=len(content),
                                         scraped_text_len=len(x_text),
                                         scraped_text=x_text,
                                         request_err=None,
                                         source_url=record['url'])

    db = get_scraping_db()
    res_tbl = db.get_table(TBL_SCRAPE_RESULTS)
    res_tbl.insert_many([res.dict() for res in results.values()])


def scrape_one(record: Series, use_cache: bool = True) -> Series:
    """Scrape a single record"""
    source_url = record['source_url']
    url_hash = record['url_hash']

    L.info('Working on url=%r use_cache=%r', source_url, use_cache)
    resp_res= get_from_s3_or_request(source_url, url_hash, use_cache=use_cache)

    if resp_res.is_success():
        scraped_len = len(resp_res.content)
        decoded_content = _decode_content(resp_res.content, resp_res.content_encoding)
        scraped_text = extract_text(decoded_content)
        scraped_text_len=len(scraped_text)
    else:
        scraped_len = 0
        scraped_text = None
        scraped_text_len = 0

    result = ScrapeResult(url_hash=url_hash,
                         source_url=source_url,
                         status_code=resp_res.status_code,
                         scraped_len=scraped_len,
                         scraped_text_len=scraped_text_len,
                         scraped_text=scraped_text,
                         request_err=resp_res.request_error,
            ).dict()
    result['part_date'] = record['heat_date']

    return pd.Series(result)



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

def extract_text(content: bytes) -> str:
    """Extract plain human readable text from html code"""
    soup = BeautifulSoup(content, features='html.parser')
    return re.sub("\n+", "\n", soup.text)



def cache_dir() -> Path:
    """Get path to scraping cache directory"""
    path = gdelt_base_data_path() / 'scrape_cache'
    path.mkdir(parents=True, exist_ok=True)

    return path


def get_scraping_db() -> Database:
    """Get db handler for scraping"""
    db_path = gdelt_base_data_path() / 'scraping.db'
    return Database(f"sqlite:///{db_path}")


def gen_url_hash(url: str) -> str:
    """Generate a length 32 byte hash for an url"""
    return sha256(url.encode("utf8")).hexdigest()[:32]


def _decode_content(content: bytes, encoding: str | None) -> bytes:
    ret: bytes = b''
    if encoding is None:
        ret = content
    elif encoding == "br":
        try:
            ret =  brotli.decompress(content)
        except Exception as err:  # noqa: BLE001
            L.error(f"Brotli decompression failed: {err!r}")
            ret = content
    elif encoding == "gzip":
        # return gzip.decompress(content)
        L.warning(f"encoding={encoding!r} won't decompress, content={content[:50]!r}...")
        ret = content
    elif encoding == "deflate":
        ret = deflate.gzip_decompress(content)
    elif encoding is not None and encoding.lower() == "utf-8":
        ret =  content
    else:
        L.warning(f"encoding={encoding!r} won't do decompression")
        ret = content

    return ret


ReqDictValueT: TypeAlias = None | int | str | dict[str, str]

@dataclass
class RequestResult:
    """Capture and enable serialization of key data from request.Response"""

    status_code: int
    headers: dict[str, str]
    content: bytes
    content_encoding: str | None
    request_error: str | None

    @classmethod
    def from_response(cls, resp: requests.Response | None,
                      request_error: str | None = None) -> Self:
        """Make an instance from requests.Response"""
        if resp is not None:
            content_bytes = (resp.content.encode("utf8") if isinstance(resp.content, str)
                             else resp.content)
            return cls(
                status_code=resp.status_code,
                headers=dict(resp.headers),
                content=content_bytes if content_bytes is not None else b"",
                content_encoding=resp.headers.get("Content-Encoding"),
                request_error=request_error,
            )
        else: # resp is None
            return cls(
                status_code=-1,
                headers={},
                content=b"",
                content_encoding=None,
                request_error=request_error,
            )


    def to_dict(self) -> dict[str, ReqDictValueT]:
        """Convert to dict"""
        return {
            "status_code": self.status_code,
            "headers": self.headers,
            "content_b85": b85encode(self.content).decode("ascii"),
            "content_encoding": self.content_encoding,
            "request_error": self.request_error,
        }

    @classmethod
    def from_dict(cls, a_dict: dict[str, ReqDictValueT]) -> Self:
        """Make an instance from a dict"""
        return cls(
            status_code=assert_type(a_dict["status_code"], int),
            headers=assert_type(a_dict["headers"], dict),
            content=b85decode(assert_type(a_dict["content_b85"], str).encode("ascii")),
            content_encoding=assert_type_or_none(a_dict["content_encoding"], str),
            request_error=assert_type_or_none(a_dict["request_error"], str),
        )

    @classmethod
    def from_json_bytes(cls, json_bytes: bytes) -> Self:
        """Deserialize from json"""
        a_dict = json.loads(json_bytes.decode("ascii"))
        return cls.from_dict(a_dict)

    def to_bytes(self) -> bytes:
        """Serialize as json"""
        json_str = json.dumps(self.to_dict())
        return json_str.encode("ascii")

    def is_success(self) -> bool:
        """Tell whether request was a success"""
        return self.status_code // 100 == 2 and len(self.content) > 0  # noqa: PLR2004

def get_from_s3_or_request(url: str, url_hash: str, use_cache: bool = True) -> RequestResult:
    """Get (contents, status_code, get request error) for a given url.

    If content is already available is in local cache (filesystem) then get it from there.
    """
    client = get_s3_client()
    s3ref = s3_existing_ref_for(client, url_hash) if use_cache else None
    if s3ref is not None:
        L.info(f"Cache hit, for url_hash={url_hash}")
        json_bytes = client.get(s3ref)
        return RequestResult.from_json_bytes(json_bytes)
    else:
        resp = None
        try:
            resp = requests.get(url, timeout=30, headers=REQ_HEADERS)
        except Exception as err:  # noqa: BLE001
            L.warning(f"request to url='{url}' raised exception={err!r}")
            return RequestResult.from_response(resp, request_error=repr(err))

        if resp.status_code // 100 == 3:  # noqa: PLR2004
            L.warning(f"Redirect! {pformat(resp.headers)}")

        resp_res = RequestResult.from_response(resp)

        success =resp_res.is_success()
        s3ref = s3_ref_for_url(url_hash, success=success)
        client.put(s3ref, resp_res.to_bytes())

        if not success:
            if resp.status_code // 100 != 4: # noqa: PLR2004
                L.warning(f"url='{url}' returned status_code={resp.status_code}\n"
                          f"content={resp.content[:50]!r}")
            else: # status 4XX
                L.warning(f"url='{url}' returned status_code={resp.status_code}\n")


        return resp_res

def s3_existing_ref_for(client: S3Client, url_hash: str) -> S3Ref | None:
    """Get existing s3 ref to retrive cached data or None if it doesn't exist"""
    s3ref_success = s3_ref_for_url(url_hash, success=True)
    if client.exists(s3ref_success):
        return s3ref_success
    del s3ref_success

    s3ref_fail = s3_ref_for_url(url_hash, success=False)
    if client.exists(s3ref_fail):
        return s3ref_fail

    return None

def  s3_ref_for_url(url_hash: str, success: bool) -> S3Ref:
    """Make and s3 ref to store/retrieve cached scraping data for this url"""
    pre_fix = "success" if success else "FAIL"

    return S3Ref(bucket=os.environ["S3_BUCKET"],
                 key= f"{os.environ['S3_KEY_PREFIX']}/scraping_cache/{pre_fix}.{url_hash}.json")


if __name__ == "__main__":
    run_scraping(batch_size=10)
