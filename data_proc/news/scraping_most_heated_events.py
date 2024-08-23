"""Scraping of news and summarization of articles corresponding to most heated events"""
import json
import os
from base64 import b85decode, b85encode
from dataclasses import dataclass
from pathlib import Path
from pprint import pformat
from typing import TypeAlias

import brotli  # type: ignore [import-untyped]

# import gzip
import deflate  # type: ignore [import-untyped]
import pandas as pd
import requests
from pandas import Series
from typing_extensions import Self

from data_proc.common import gdelt_base_data_path

# from pyspark.sql import SparkSession
from data_proc.news.scraping import ScrapeResult, extract_text, get_most_heated_events
from shared import DataFrame, assert_type, assert_type_or_none, date, logging, read_env
from shared.s3_utils import S3Client, S3Ref, get_s3_client

L = logging.getLogger("heated_sum")
# %%
SparkSession: TypeAlias = None

REQ_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    # " (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept": "text/html,application/xhtml+xml",
    "Referer": "http://www.google.com",
}
# %%


def _interactive_testing(heated_events: DataFrame) -> None:
    # %%
    from shared import runpyfile
    read_env("/home/teo/profile.sinapsis.env")
    runpyfile("data_proc/news/scraping_most_heated_events.py")
    heat_date = date(2024, 8, 8)
    heated_events = get_most_heated_events(heat_date=heat_date, top_k=3)
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
                         request_err=resp_res.request_error
            ).dict()
    result['part_date'] = record['heat_date']

    return pd.Series(result)


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
