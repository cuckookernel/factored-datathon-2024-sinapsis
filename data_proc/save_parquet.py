"""Saving parquets..."""
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
from pandas import DataFrame

from data_proc.common import GdeltV1Type
from data_proc.load import interpret_fname
from shared import logging

L = logging.getLogger("save_pq")

@dataclass
class SaveParquetStats:
    """Cnts of stuff kept while saving parquets"""

    raw_file_cnt: int =0
    raw_bytes_read: int = 0
    row_cnt: int = 0
    parquet_file_cnt: int = 0
    parquet_bytes_written: int = 0
    parquet_rows_written: int = 0

    def inc_raw(self, file_cnt: int,
            bytes_read: int, row_cnt: int) -> None:
        """Increment raw data reading stats"""
        self.raw_file_cnt += file_cnt
        self.raw_bytes_read += bytes_read
        self.row_cnt += row_cnt

    def inc_parquet(self, file_cnt: int, bytes_written: int, row_cnt: int) -> None:
        """Increment parquet writing stats"""
        self.parquet_file_cnt += file_cnt
        self.parquet_bytes_written += bytes_written
        self.parquet_rows_written += row_cnt

    def log(self) -> None:
        """Log the stats"""
        L.info("%r compression:%.4g", self,
               self.parquet_bytes_written / self.raw_bytes_read)


class ParquetChunkGenerator:
    """Iterate over raw files in src_path, convert to parquet while grouping them, keep stats"""

    def __init__(self, typ: GdeltV1Type, src_path: Path) -> None:
        self.typ = typ
        self.src_path = src_path
        self.dst_dir_path = src_path.parent / 'raw_parquet'
        self.dst_dir_path.mkdir(exist_ok=True, parents=True)

        self.type_suffix = "export" if typ == "events" else typ

        self.chunk_dfs: list[DataFrame] = []
        self.date_strs: list[str] = []
        self.ret_stats = SaveParquetStats()
        self.chunk_idx = 0

    def save_parquet_chunks(self, rows_per_file: int,
                            raw_df_iter: Generator[tuple[DataFrame, Path], None, None],
                            limit: Optional[int] = None,
                            verbose: int = 0) -> SaveParquetStats:
        """Convert raw files under src_path to parquets trying to consolidate at least rows_per_count rows in each parquet""" # noqa: E501
        chunk_row_cnt = 0

        sampled_suffix = "undefined"  # Will be defined if we actually need it below
        for i, (df, path) in enumerate(raw_df_iter):
            date_str, sampled_suffix = interpret_fname(path)
            self.date_strs.append(date_str)
            self.chunk_dfs.append(df)
            chunk_row_cnt += df.shape[0]
            self.ret_stats.inc_raw(file_cnt=1,
                                   bytes_read=path.lstat().st_size,
                                   row_cnt=df.shape[0])

            if chunk_row_cnt > rows_per_file or (limit is not None and i >= limit):
                self._save_1_parquet_chunk(sampled_suffix, verbose)
                chunk_row_cnt = 0

            if limit is not None and i >= limit:
                break

        if len(self.chunk_dfs) != 0:
            self._save_1_parquet_chunk(sampled_suffix, verbose)

        return self.ret_stats

    def _save_1_parquet_chunk(self, sampled_suffix: str, verbose: int = 0) -> None:
        chunk_df_out: DataFrame = pd.concat(self.chunk_dfs)
        assert isinstance(chunk_df_out, DataFrame)  # noqa: S101 - for typecheckers benefit
        fname_out = (f"{min(self.date_strs)}-{max(self.date_strs)}"
                     f".{self.type_suffix}{sampled_suffix}.parquet")
        chunk_path_out = self.dst_dir_path / fname_out

        if verbose > 0:
            L.info("Saving parquet chunk: %s", chunk_path_out)
        chunk_df_out.to_parquet(chunk_path_out)

        self.ret_stats.inc_parquet(file_cnt=1,
                                   bytes_written=chunk_path_out.lstat().st_size,
                                   row_cnt=chunk_df_out.shape[0])
        if verbose > 1:
            self.ret_stats.log()

        self.chunk_dfs = []
        self.date_strs = []
        self.chunk_row_cnt = 0


def save_parquet(df: DataFrame, fnames: list[str], dst_path: Path,
                  verbose: int = 0) -> None:
    """Save a DataFrame to a Parquet file based on the provided filenames and destination path.

    (No longer used, perhaps...)

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
