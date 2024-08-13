#!/usr/bin/env python3
"""Command line script to download raw CSV.zip files from GDELT dataset"""

from datetime import datetime as DateTime
from pathlib import Path

import click

from data_proc.common import GdeltV1Type
from data_proc.download import download_files, raw_files_list
from shared import logging

L = logging.getLogger("dl_raw_csvs")

@click.command()
@click.argument('file_type', type=click.Choice(["events", "gkg", "gkgcounts"]))
@click.argument('start_date',  type=click.DateTime())
@click.argument('end_date',  type=click.DateTime())
@click.argument('dst_path', type=click.Path(path_type=Path), required=True)
@click.option('-f', '--sample_fraction', type=float, default=1.0)
@click.option('-s', '--random_seed', type=float, default=1.0)
@click.option('-v', '--verbose', default=0)
def download_csvs_v1(file_type: GdeltV1Type,
                     start_date: DateTime, end_date: DateTime,
                     dst_path: Path,
                     sample_fraction: float,
                     random_seed: int,
                     verbose: int) -> None:
    """Download raw CSV files from GDELT v1.0 for a given date range.

    Args:
    ----
    file_type (Gdelt2FileType): The type of file to download (events, mentions, gkg).
    start_date (Date): The start date for the data retrieval.
    end_date (Date): The end date for the data retrieval.
    dst_path (Path): The directory to save the downloaded files.
    sample_fraction (float, optional): The fraction (in [0.0, 1.0])
            of files in the interval to download. Defaults to 1.0
    random_seed (int, optional, default=0): Control exactly what sample to produce.
    verbose (int, optional): Verbosity level for output. Defaults to 0.

    """
    url_list = raw_files_list(start=start_date.date(), end=end_date.date(),
                              typ=file_type)

    L.info(f"url_list has: {len(url_list)} items\nfirst: {url_list[0]}\nlast:  {url_list[-1]}\n")

    raw_dst_path = dst_path / 'raw_data'
    del dst_path
    raw_dst_path.mkdir(exist_ok=True, parents=True)
    L.info(f"Output dir is: {raw_dst_path}")

    download_files(url_list=url_list, dst_dir=raw_dst_path, verbose=verbose,
                   sample_fraction=sample_fraction, random_seed=random_seed)



if __name__ == "__main__":
    download_csvs_v1()
