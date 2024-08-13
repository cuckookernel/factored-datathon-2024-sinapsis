"""Utilities to download the raw GDELT v2 data"""

import random
import re
import time
import zipfile
from datetime import date as Date
from datetime import datetime as DateTime
from datetime import timedelta as TimeDelta
from http import HTTPStatus
from io import BytesIO
from pathlib import Path
from typing import Optional
from zipfile import ZipFile

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from pandas import DataFrame

import shared
from data_proc.common import UTC, GdeltV1Type, gdelt_base_data_path
from shared import logging

L = logging.getLogger("download")

BASE_URL = "http://data.gdeltproject.org"
# %%


def _interactive_testing() -> None:
    # %%
    typ: GdeltV1Type = "gkg"
    shared.read_env()
    # %%
    get_file_catalog(typ)
    # %%
    cat_events = get_file_catalog("events", redownload=True)
    cat_gkg = get_file_catalog("gkg", redownload=True)
    cat_gkgcounts = get_file_catalog("gkgcounts", redownload=True)
    print(cat_events.shape, cat_gkg.shape, cat_gkgcounts.shape)
    print("Done")
    # %%


def get_file_catalog(typ: GdeltV1Type, redownload: bool = False) -> DataFrame:
    """Get file catalog either from local disc or via download_file_catalog()"""
    url_segment = "gkg" if typ == "gkgcounts" else typ
    local_catalog_path: Path = gdelt_base_data_path() / f"file_catalog.{typ}.parquet"

    if local_catalog_path.exists():
        modif_time = DateTime.fromtimestamp(local_catalog_path.lstat().st_mtime, tz=UTC)

        if (DateTime.now(tz=UTC) - modif_time).total_seconds() < 3 * 3600 and not redownload:
            L.info("Loading catalog for typ: %s from:'%s' (modified: %s)",
                   typ, local_catalog_path, modif_time.isoformat())
            return pd.read_parquet(local_catalog_path).set_index("date_str")

    type_base_url = f"{BASE_URL}/{url_segment}"
    catalog_df = download_file_catalog(type_base_url, typ)
    catalog_df.to_parquet(local_catalog_path)
    csv_path = local_catalog_path.with_suffix('.csv')
    catalog_df.to_csv(csv_path, index=False)

    L.info(f"Catalog for typ: {typ} saved to: {local_catalog_path}\nand also to: {csv_path}")

    return catalog_df.set_index("date_str")
# %%

def download_file_catalog(type_base_url: str, typ: GdeltV1Type) -> DataFrame:
    """Download file catalog from gdelt v1 data server"""
    index_url = f"{type_base_url}/index.html"

    resp = requests.get(index_url, timeout=30)
    if resp.status_code != HTTPStatus.OK:
        raise RuntimeError(f"request to index url:`{index_url}` failed\nresp:{resp}")

    L.info(f"Downloaded catalog from index url={index_url}, reponse_size={len(resp.content)}")
    # Parse the HTML response
    catalog_df = parse_html(resp, typ)
    catalog_df['full_url'] = type_base_url + "/" + catalog_df['filename']
    catalog_df['event_type'] = typ
    return catalog_df


# %%

def parse_html(resp: requests.Response, typ: GdeltV1Type) -> DataFrame:
    """Parse catalog to get file names, sizes and md5"""
    lines = resp.content.split(b"\n")
    def fix_li_line(line: bytes) -> bytes:
        if line.startswith(b"<LI>") and not line.endswith(b"</LI>"):
            return line + b"</li>"
        else:
            return line

    new_lines = [fix_li_line(line) for line in lines]

    soup: Tag = BeautifulSoup(b"\n".join(new_lines), features="html.parser")
    assert isinstance(soup, Tag) # noqa: S101 # needed for type checker
    bullet_list = soup.find(name="ul")
    assert isinstance(bullet_list, Tag) # noqa: S101 # needed for type checker
    items: list[Tag] = bullet_list.find_all(name="li")
    L.info("bullet list has %d", len(items))

    records: list[dict[str, str]] = []

    file_suffix = {"events": "export.CSV.zip",
                   "gkg": "gkg.csv.zip",
                   "gkgcounts": "gkgcounts.csv.zip"}[typ]

    for item in items:
        anchor = item.find(name="a")
        assert isinstance(anchor, Tag) # noqa: S101 # needed for type checker
        text = item.getText()
        filename = anchor.attrs["href"]
        regex = (r"(?P<date_str>[0-9]+)(\." + file_suffix + r")? "
                 r"\((?P<size_mb>[0-9.]+)MB\) *\(MD5: (?P<md5>[0-9a-f]+)\)")
        mch = re.match(regex, text)
        if mch:
            record = {
                'date_str': mch.group('date_str'),
                'filename': filename,
                'size_mb': mch.group('size_mb'),
                'md5': mch.group('md5'),
            }
            records.append(record)

    catalog_df = DataFrame(records)
    L.info("Catalog for typ=%s has: %s", typ, catalog_df.shape)

    return catalog_df
# %%

def raw_files_list(*, start: Date, end: Date,
                   typ: GdeltV1Type) -> list[str]:
    """Generate a list of raw file URLs based on the specified start and end dates, file type (events, gkg, or mentions), and verbosity level.

    Args:
    ----
    start (date): The start date.
    end (date): The end date.
    typ (str): The type of file to retrieve (events, gkg, or mentions).

    Returns:
    -------
    - A list of raw file URLs.

    """  # noqa: E501
    catalog_df = get_file_catalog(typ)
    ret: list[str] = []

    for ts in timestamps_list_gdelt_v1(start, end):
        full_url = catalog_df.loc[ts, "full_url"]
        assert isinstance(full_url, str)  # noqa: S101 # needed for typechecker
        ret.append(full_url)

    return ret


def timestamps_list_gdelt_v1(start: Date, end: Date) -> list[str]:
    """Generate a list of dates (formatted as YYYYMMDD) between the start and end dates.

    Args:
    ----
    start (Date): The starting date.
    end (Date): The ending date.
    sample_fraction (float, optional): The sampling fraction
    random_seed (int, optional, default=0): Control exactly what sample to get.

    Returns:
    -------
    list[str]: A list of timestamps in the format 'YYYYMMDD'.

    """
    ret: list[str] = []
    date = start

    while date < end:
        date_str = date.strftime("%Y%m%d")
        ret.append(f"{date_str}")

        date += TimeDelta(1.0)

    return ret


# %%
def download_files(url_list: list[str], dst_dir: Path,
                   sample_fraction: float = 1.0,
                   random_seed: int = 0,
                   verbose: int = 0) -> tuple[int, float, TimeDelta]:
    """Download files from a list of URLs to a destination path.

    Args:
    ----
        url_list (list[str]): List of URLs to download files from.
        dst_dir (Path): Destination path to save the downloaded files.
        sample_fraction (float): If < 1.0, sample this fraction of lines
        random_seed (int): what random seed to use
        verbose (int): How much output to produce

    Returns:
    -------
        Tuple[int, float, TimeDelta]: A tuple containing the number of files downloaded,
                total size in MiB, and elapsed time.

    """
    start_time = DateTime.now(tz=UTC)
    mbs_downloaded: float = 0.0
    mbs_written: float = 0.0

    already_there = 0
    download_cnt = 0

    if len(url_list) == 0:
        L.warning("download_files for empty url_list - did NOTHING")
        return 0, 0.0, start_time - start_time

    filename = ""

    random.seed(random_seed)
    for url in url_list:
        filename = Path(url).name

        dst_file_path = get_dst_file_path(dst_dir, filename, sample_fraction)
        if dst_file_path.exists() and dst_file_path.lstat().st_size > 0:
            # Already there, not downloading...
            already_there += 1
            continue

        # Actually download:
        resp = download_one(url)
        if resp is None:
            continue

        download_cnt += 1
        if verbose > 1:
           L.info("Downloaded: %s ... (size: %d)", filename, len(resp.content) )

        if len(resp.content) > 0:
            mbs_written += _write_one(resp.content, sample_fraction, dst_file_path) / 1e6
        total = download_cnt + already_there
        if  total % 10 == 0 and total > 0 and verbose > 0:
            log_counters(len(url_list), download_cnt, already_there,
                         mbs_downloaded, mbs_written, start_time, filename)

        mbs_downloaded += len(resp.content) / 1e6

    if verbose > 0:
        log_counters(len(url_list), download_cnt, already_there, mbs_downloaded, mbs_written,
                     start_time, filename)

    L.info(f"Done writing files to: {dst_dir}")
    return (len(url_list), mbs_downloaded,
            DateTime.now(tz=UTC) - start_time)


def get_dst_file_path(dst_dir: Path, filename: str, sample_fraction: float) -> Path:
    """Construct the local destination file name depending on whether sample_fraction < 1.0"""
    unsampled = dst_dir / filename
    if sample_fraction < 1.0:
        suffix = f".sampled_{sample_fraction:.4g}.zip"
        return unsampled.with_suffix(suffix)
    else:
        return unsampled


def download_one(url: str,
                 max_retries: int = 10) -> Optional[requests.Response]:
    """Download data from the given URL.,

    Args:
    ----
    url : to download
    max_retries: max number of retries if error (other than 404)

    Returns:
    -------
    The response if successful,
    or None if the file is not found. Retries if errors occur.

    """
    filename = Path(url).name
    error = True
    retries = 0

    while error and retries < max_retries:
        retries += 1
        try:
            resp = requests.get(url, timeout=30)
            error = resp.status_code != HTTPStatus.OK or len(resp.content) == 0
        except requests.Timeout:
            L.warning("Timeout occurred, will retry")
            error = True
            continue

        if error:
            if resp.status_code == HTTPStatus.NOT_FOUND:
                L.info(f"File `{filename}` - NOT FOUND (404) skipping ")
                return None
            else:
                L.warning(f"{filename}  status_code={resp.status_code} len: {len(resp.content)} "
                         f"trying again in 5 seconds")
            time.sleep(5)
        else:
            return resp

    return None



def _write_one(zipped_data: bytes, sample_fraction:  float, dst_file_path: Path) -> int:
    if len(zipped_data) == 0:
        L.warning("No data, nothing to do")
        return 0

    orig_size = len(zipped_data)
    if sample_fraction < 1.0:
        in_zip = ZipFile(BytesIO(zipped_data))
        names = in_zip.namelist()

        if len(names) != 1:
            raise ValueError(f"name={names}, expected only one element")

        data = in_zip.read(name=names[0])
        lines = data.split(b"\n")
        n_lines = len(lines)
        k = int(n_lines * sample_fraction)
        sample_lines = random.sample(lines, k=k)
        L.debug("n_lines=%d k=%d len(sample_lines)=%d", n_lines, k, len(sample_lines))
        data_out = b"\n".join(sample_lines)

        with ZipFile(dst_file_path, "w", compression=zipfile.ZIP_LZMA,
                     compresslevel=7) as f_out:
            f_out.writestr(names[0], data=data_out)

        new_size = dst_file_path.lstat().st_size
        L.debug("dst_file: '%s', orig_size=%d new_size=%d ratio=%.4f",
                dst_file_path.name, orig_size, new_size, new_size/orig_size)
        return new_size

    else:
        with dst_file_path.open("wb") as f_out:
            f_out.write(zipped_data)
        return orig_size


def log_counters(total_cnt: int,
                 downloaded_cnt: int, already_there: int,
                 mbs_downloaded: float, mbs_written: float, start_time: DateTime,
                 last_fname: str) -> None:
    """Log download progress"""
    end_time = DateTime.now(tz=UTC)
    elapsed_secs = (end_time - start_time).total_seconds()
    download_rate = mbs_downloaded / elapsed_secs

    ratio = mbs_written / mbs_downloaded

    progress = (downloaded_cnt + already_there) / total_cnt
    L.info(f"Progress: {progress * 100:.1f} % | "
           f"Files downloaded: {downloaded_cnt} | "
           f"not downloaded (were already there): {already_there} | "
           f"last file: '{last_fname}' | "
           f"total downloaded: {mbs_downloaded:.1f} MiB | "
           f"total written: {mbs_written:.1f} MiB | "
           f"written / downloaded: {ratio:.3g} | "
           f"elapsed time: {elapsed_secs:.2f} secs | "
           f"download rate: {download_rate:.2f} MiB/s")


# %%


def _interactive_run_download() -> None:

    # %%
    url_list = raw_files_list(
                start=Date(2024, 8, 2), end=Date(2024, 8, 10),
                typ="events")

    # %%
    import os

    from dotenv import load_dotenv

    load_dotenv(f"{os.environ['HOME']}/profile.env")
    # %%
    data_path = Path(os.environ['DATA_DIR'])
    dst_path = data_path / 'GDELT/raw_data'

    download_files(url_list=url_list, dst_dir=dst_path)
    # %%
