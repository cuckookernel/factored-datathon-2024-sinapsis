#!/usr/bin/env python
"""Command line utility to convert a raw data files to parquet"""

from pathlib import Path
from typing import Optional

import click
from pandas import DataFrame

from data_proc.common import GdeltV1Type
from data_proc.load import save_parquet_chunks


@click.command()
@click.argument('file_type', type=click.Choice(["events", "gkg", "gkgcounts"]))
@click.argument('src_path', type=click.Path(exists=True, path_type=Path))
@click.option('-r', '--rows_per_file', type=int, default=250000)
@click.option('-l', '--limit', default=None)
@click.option('verbose', '-v', default=1)
def generate_parquets(file_type: GdeltV1Type,
                      src_path: Path,
                      rows_per_file: int,
                      limit: Optional[int],
                      verbose: int) -> None:
    """Generate parquet files from raw CSV files in a given directory.

    Args:
    ----
        file_type: Gdelt2FileType - The type of GDELT2 file to process.
        src_path: str - The directory where the raw .CSV.zip files are stored.
        verbose: int - Verbosity level (default is 0).

    """
    src_path1 = src_path / 'raw_data'

    stats = save_parquet_chunks(file_type, rows_per_file, src_path=src_path1, limit=limit,
                                verbose=verbose)
    stats.log()



def save_parquet(df: DataFrame, fnames: list[str], dst_path: Path,
                  verbose: int = 0) -> None:
    """Save a DataFrame to a Parquet file based on the provided filenames and destination path.

    Args:
    ----
        df (DataFrame): The DataFrame to be saved.
        fnames (list[str]): List of filenames to extract timestamps from.
        dst_path (Path): Destination path to save the Parquet file.
        verbose (int, optional): Verbosity level, default 0

    Returns:
    -------
        None

    """
    tstamps = sorted([ fname.split('.')[0][2:-2] for fname in fnames ])
    suffix = fnames[0].split('.')[1]
    if not dst_path.exists():
        print(f'save_parquet: creating path: {dst_path}')
        dst_path.mkdir(exist_ok=True, parents=True)

    parquet_fpath = dst_path / f"{tstamps[0]}-{tstamps[-1]}.{suffix}.parquet"
    df.to_parquet(parquet_fpath)

    if verbose > 0:
        print(f'save_parquet: created file {parquet_fpath}, data shape: {df.shape}')



if __name__ == "__main__":
    generate_parquets()
