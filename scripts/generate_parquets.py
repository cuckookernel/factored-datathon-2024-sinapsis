#!/usr/bin/env python
"""Command line utility to convert a raw data files to parquet"""

from pathlib import Path
from typing import Optional

import click

from data_proc.common import GdeltV1Type
from data_proc.load import df_iter_from_raw_files
from data_proc.save_parquet import ParquetChunkGenerator


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
        limit: int - generate at most this many files (use for quick tests)
        rows_per_file: int - Try to produce parquet files with at least this many lines each
        verbose: int - Verbosity level (default is 0).

    """
    src_path1 = src_path / 'raw_data'

    raw_df_iter = df_iter_from_raw_files(file_type, src_path=src_path1)
    pq_generator = ParquetChunkGenerator(typ=file_type, src_path=src_path1)

    stats = pq_generator.save_parquet_chunks(rows_per_file=rows_per_file,
                                             raw_df_iter=raw_df_iter,
                                             limit=limit,
                                             verbose=verbose)

    stats.log()


if __name__ == "__main__":
    generate_parquets()
