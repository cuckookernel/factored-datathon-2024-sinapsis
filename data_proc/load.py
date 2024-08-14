"""Utilities to load data from disk into Dataframes  and set the schema on those"""
# %%
import os
import re
import zipfile
from collections.abc import Generator, Mapping, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional, TypeAlias

import numpy as np
import pandas as pd
from numpy import dtype
from pandas import DataFrame

from data_proc.common import GdeltV1Type, gdelt_base_data_path, NaN
from shared import logging

L = logging.getLogger("load")

ColNameMode: TypeAlias = Literal["orig", "snake_case"]
# %%

def _interactive_run_load() -> None:
    # %%
    # noinspection PyUnresolvedReferences
    runfile("data_proc/load.py") # noqa: F821
    # %%
    import sys

    from dotenv import load_dotenv
    load_dotenv(f'{os.environ["HOME"]}/profile.env')
    sys.path.append('.')
    # %%
    # data_dir = Path(os.environ['DATA_DIR'])
    # gdelt_data_dir = data_dir / 'GDELT'
    typ: GdeltV1Type = "gkg"
    print(typ)
    # %%
    schema_df = load_schema(typ=typ)
    cols = schema_df['column']
    print(cols)
    # %%
    rows_per_chunk = 100000
    src_path = gdelt_base_data_path() / 'events_aug2024'
    # %%
    csv_paths =list((src_path / 'raw_data').glob('*.zip'))
    csv_path = csv_paths[0]
    # %%
    one_df = pd.read_csv(csv_path, sep='\t', names=list(schema_df['column']))
                        # dtype={"GLOBALEVENTID": int})
    dtype_map = dict(zip(schema_df['column'], schema_df['pandas_type'], strict=False))
    print(dtype_map)
    # %%
    save_parquet_chunks(typ, rows_per_chunk, src_path / 'raw_data', limit=100,
                        verbose=1)
    # %%
    for col in one_df.columns:
        cnt = (one_df[col] == 'GOV').sum()

        if cnt > 0:
            print(f"{col}: {cnt}")
    # %%
    for _, row in schema_df[['column', 'snake_col_name']].iterrows():
        print(list(row))
# %%

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
    def __init__(self, typ: GdeltV1Type, src_path: Path):
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
                            limit: Optional[int] = None,
                            verbose: int = 0) -> SaveParquetStats:
        """Convert raw files under src_path to parquets trying to consolidate at least rows_per_count rows in each parquet""" # noqa: E501


        chunk_row_cnt = 0

        sampled_suffix = "undefined"  # Will be defined if we actually need it below
        for i, (df, path) in enumerate(df_iter_from_raw_files(self.typ, src_path=self.src_path)):
            date_str, sampled_suffix = interpret_fname(path)
            self.date_strs.append(date_str)
            self.chunk_dfs.append(df)
            chunk_row_cnt += df.shape[0]
            self.ret_stats.inc_raw(file_cnt=1,
                                   bytes_read=path.lstat().st_size,
                                   row_cnt=df.shape[0])

            if chunk_row_cnt > rows_per_file or (limit is not None and i >= limit):
                self._save_1_parquet_chunk(sampled_suffix, verbose)

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


def df_iter_from_raw_files(typ: GdeltV1Type,
                           src_path: Path,
                           column_name_mode: ColNameMode = "snake_case",
                           verbose: int = 0,
                           ) -> Generator[tuple[DataFrame, Path], None, None]:
    """Load raw data files based on the specified GDELT type and schema.

    Raw data here means '.CSV.zip' files.

    Args:
    ----
        typ: Type of GDELT data to load (events, gkg, mentions).
        rename_cols: Boolean flag to indicate whether to rename columns based on schema.
        src_path: Path to the raw data files.
        column_name_mode: Whether to use original names or camel case names
        raw_fpaths: Optional list of paths to raw data files.
        verbose (int): Verbosity level

    Returns:
    -------
    A tuple containing a DataFrame with concatenated data and a list of file names.

    """
    # %%
    schema_df = load_schema(typ)
    suffix = "export" if typ == "events" else typ

    glob_patterns = [f'*.{suffix}.CSV.zip', f'*.{suffix}.csv.zip',
                     f'*.{suffix}.CSV.sampled_*.*.zip', f'*.{suffix}.csv.sampled_*.*.zip']
    raw_fpaths = sorted([fpath for pat in glob_patterns for fpath in src_path.glob(pat)])
    if len(raw_fpaths) == 0:
        L.warning("raw_paths is empty, src_path='%s', glob_patterns=%s",
                  src_path, glob_patterns)
    # %%

    col_names, dtype_map, converters = get_cols_and_types(schema_df, column_name_mode)

    L.info(f"col_names=%s, converters=%s", col_names, converters)

    for fpath in raw_fpaths:
        try:
            if fpath.lstat().st_size == 0:
                L.info(f"WARN: Skipping empty file: {fpath}")
                continue
            if typ == 'events':
                interval_df = pd.read_csv(fpath, sep='\t', names=col_names,
                                          dtype=dtype_map, converters=converters)
            else:
                interval_df = pd.read_csv(fpath, sep='\t', names=col_names,
                                          dtype=dtype_map, header=1)

        except zipfile.BadZipFile:
            L.info(f"BadZipFile exception for {fpath} (size={fpath.lstat().st_size})")
            continue
        except ValueError as err:
            if err.args[0].startswith('Zero files found'):
                L.warning("Zero files found in fpath: %s, skipping", fpath)
                continue
            else:
                L.error(f"When reading: {fpath}\nerror msg: {err.args[0]}")
                diagnose_problem(fpath, col_names, dtypes=dtype_map)
                raise
        except Exception as exc:
            L.error("Exception reading parquet from path: %s\n%s", fpath, exc.args)
            raise

        if verbose > 1:
            L.info(f'fname: {fpath.name} - {interval_df.shape[0]} records')

        yield interval_df, fpath


def ensure_float(a_str: str) -> float:
    """Converter to ensure we get a float from a string (when loading csv)"""
    if a_str == '':
        return NaN
    try:
        return float(a_str)
    except ValueError as err:
        L.info(f"a_str=%s is not a float, attempting cleanup", a_str)
        a_clean = re.sub("[^0-9-.]", "", a_str)
        try:
            return float(a_clean)
        except ValueError as a_str:
            L.info("a_str=%s is not a float after cleanup", a_clean)
            return NaN

def diagnose_problem(fpath: Path, col_names: list[str], dtypes: dict[str, dtype]):
    n_cols = len(col_names)
    L.info(f"col_names has: {n_cols}: {col_names}")
    data_df = pd.read_csv(fpath, sep="\t")
    n_cols_file = data_df.shape[1]
    L.info(f"file has: {n_cols_file} columns")
    first_row = data_df.iloc[0]
    L.info(f"first row: {list(first_row)}")

    if n_cols == n_cols_file:
        data_df = pd.read_csv(fpath, names=col_names, sep="\t")
        print("row 1", data_df.iloc[1])

        for col,dtyp in dtypes.items():
            if dtyp == np.float64:
                try:
                    float_series = data_df[col].astype(np.float64)
                    L.info("col: `%s` # ok values: %d", col, (~float_series.isnull()).sum())
                except ValueError as err:
                    L.error("col: `%s`  error: %s", col, err.args[0])


def interpret_fname(path: Path) -> tuple[str, str]:
    """Extract date_str and sample suffix from filename"""
    fname = path.name
    date_str = fname.split('.')[0]
    mch = re.search(r'(\.sampled_[0-9.]+)\.zip', fname)
    if mch:
        return date_str, mch.group(1)
    else:
        return date_str, ""

def get_cols_and_types(schema_df: DataFrame,
                       col_name_mode: ColNameMode
                       ) -> tuple[list[str], dict[str, dtype], dict[str, Callable]]:
    """Extract column names and their corresponding data types from a schema DataFrame.

    Args:
    ----
        schema_df (DataFrame): The DataFrame containing schema information.
        col_name_mode (ColNameMode): A flag indicating whether to use original or
            snake_case column names.

    Returns:
    -------
        tuple[list[str], dict[str, type]]: A tuple containing a list of column names
        and a dictionary mapping column names to their respective data types.

    """
    col_names: list[str] = list(schema_df['snake_col_name']
                                if col_name_mode == 'snake_case'
                                else schema_df['column'])

    col_2_type_desc: dict[str, str] = dict(zip(col_names, schema_df["pandas_type"], strict=True))
    dtype_map = {col: TYPE_DESC_TO_NP_TYPE[pandas_type_desc]
                 for col, pandas_type_desc in col_2_type_desc.items()
                 if not pandas_type_desc.startswith('float')}

    converters = {
        col: ensure_float
        for col, pandas_type_desc in col_2_type_desc.items()
        if pandas_type_desc.startswith('float')
    }

    return col_names, dtype_map, converters


GDELT2_TYPE_DESC_MAPPING = {
    "INTEGER": "int64",
    "STRING": "str",
    "HALF": "float16",
    "FLOAT": "float64",
    "DOUBLE": "float64",
}


TYPE_DESC_TO_NP_TYPE: dict[str, dtype] = {
    "int64": np.dtype('int64'),
    "str": np.dtype(str),
    "float16": np.dtype('float16'),
    "float32": np.dtype('float32'),
    "float64": np.dtype('float64'),
}

# %%
def schema_path(typ: GdeltV1Type) -> Path:
    return Path(f"docs/schema_files/GDELT_v1.{typ}.columns.csv")


def load_schema(typ: GdeltV1Type) -> DataFrame:
    """Load the schema for GDELT 1.0 data based on the specified type.

    Args:
    ----
        typ (Gdelt2FileType): The type of GDELT data to load the schema for.

    Returns:
    -------
        DataFrame: The schema DataFrame with renamed columns and mapped data types.

    """
    # %%
    local_path = schema_path(typ)
    schema_df = pd.read_csv(local_path)
    # %%

    if 'col_renamed' in schema_df:
        schema_df['snake_col_name'] = schema_df['col_renamed']
    else:
        col_renames = {
            "GLOBALEVENTID": "ev_id",
            "SQLDATE": "date_int",
            "MONTH_YEAR": "date_int",
            "DATEADDED": "date_added",
            "SOURCEURL": "source_url",
        }

        schema_df['snake_col_name'] = (schema_df['column']
            .apply(lambda col: rename_col(col, col_renames))
        )

    schema_df['pandas_type'] = (
        schema_df['data_type'].apply(lambda data_type: GDELT2_TYPE_DESC_MAPPING[data_type])
    )

    return schema_df


def rename_col(col: str, col_renames: Mapping[str, str]) -> str:
    """Rename a column based on a mapping or convert it to snake_case if not found.

    Args:
    ----
    col : str
        The column name to be renamed.
    col_renames : Mapping[str, str]
        A mapping of original column names to new column names.

    Returns:
    -------
    str
        The renamed column name or the column name converted to snake_case if
        not found in col_renames.

    """
    if col in col_renames:
        return col_renames[col]
    else:
        return camel_to_snake_case(col)

def camel_to_snake_case(identifier: str) -> str:
    """Convert an identifier from camelCase to snake_case

    Args:
    ----
    identifier (str): The identifier to transform

    Examples:
    --------
        camel_to_snake_case("MonthYear") => "month_year"
        camel_to_snake_case("ActionGeo_Lat") => "action_geo_lat"
        camel_to_snake_case("Actor2Religion1Code") => "actor2_religion1_code"

    """
    step1 = re.sub(r'ID', 'Id', identifier)
    step1b = re.sub(r'ADM', 'Adm', step1)
    step2 = re.sub(r'(?<!^)(?=[A-Z])', '_', step1b).lower()

    return re.sub(r'_{2,}', '_', step2)
