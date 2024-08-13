"""Utilities to load data from disk into Dataframes  and set the schema on those"""
# %%
import os
import re
import zipfile
from pathlib import Path
from typing import Literal, TypeAlias, Mapping, Generator, Optional

import numpy as np
from numpy import dtype
import pandas as pd
from pandas import DataFrame

from data_proc.common import GdeltV1Type, gdelt_base_data_path
from shared import logging

L = logging.getLogger("load")

ColNameMode: TypeAlias = Literal["orig", "snake_case"]
# %%

def _interactive_run_load() -> None:
    # %%
    # noinspection PyUnresolvedReferences
    runfile("data_proc/load.py")
    # %%
    import sys

    from dotenv import load_dotenv
    load_dotenv(f'{os.environ["HOME"]}/profile.env')
    sys.path.append('.')
    # %%
    # data_dir = Path(os.environ['DATA_DIR'])
    # gdelt_data_dir = data_dir / 'GDELT'
    typ: GdeltV1Type = "gkgcounts"
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
    df = pd.read_csv(csv_path, sep='\t', names=list(schema_df['column']),)
                     # dtype={"GLOBALEVENTID": int})
    dtype_map = dict(zip(schema_df['column'], schema_df['pandas_type']))
    # %%
    save_parquet_chunks(typ, rows_per_chunk, src_path / 'raw_data', limit=100,
                        verbose=1)
    # %%
    for col in df.columns:
        df[col] = df[col].astype('str')

    # %%
    data = []
    with Path('/home/teo/data/GDELT/20220915.export.CSV').open('rt') as f_int:
        for i, line in enumerate(f_int.readlines()):
            parts = line.split("\t")
            if len(parts) != 58:
                print(len(parts), parts)
            data.append(parts)
            if line.startswith('GOV'):
                print(f"line: {i}  `{line}`")
    # %%
    df_man = pd.DataFrame(data, columns=list(schema_df['column']))
    # %%
    for i, row in enumerate(data):
        if row[0] == 'GOV':
            print(i, row)


    # %%
    for col in df.columns:
        cnt = (df[col] == 'GOV').sum()

        if cnt > 0:
            print(f"{col}: {cnt}")
    # %%
    for _, row in schema_df[['column', 'snake_col_name']].iterrows():
        print(list(row))
# %%
from dataclasses import dataclass

@dataclass
class SaveParquetStats:
    raw_file_cnt: int =0
    raw_bytes_read: int = 0
    row_cnt: int = 0
    parquet_file_cnt: int = 0
    parquet_bytes_written: int = 0
    parquet_rows_written: int = 0

    def inc_raw(self, file_cnt: int,
            bytes_read: int, row_cnt: int) -> None:

        self.raw_file_cnt += file_cnt
        self.raw_bytes_read += bytes_read
        self.row_cnt += row_cnt

    def inc_parquet(self, file_cnt: int, bytes_written: int, row_cnt: int) -> None:
        self.parquet_file_cnt += file_cnt
        self.parquet_bytes_written += bytes_written
        self.parquet_rows_written += row_cnt

    def log(self):
        L.info("%r compression:%.4g", self,
               self.parquet_bytes_written / self.raw_bytes_read)

def save_parquet_chunks(typ: GdeltV1Type,
                        rows_per_chunk: int,
                        src_path: Path,
                        limit: Optional[int],
                        verbose: int = 0) -> SaveParquetStats:

    dst_dir_path = src_path.parent / 'raw_parquet'
    dst_dir_path.mkdir(exist_ok=True, parents=True)

    type_suffix = "export" if typ == "events" else typ
    chunk_idx = 0
    chunk_row_cnt = 0
    chunk_dfs = []
    date_strs = []

    ret_stats = SaveParquetStats()

    for i, (df, path) in enumerate(df_iter_from_raw_files(typ, src_path=src_path)):
        date_str, sampled_suffix = interpret_fname(path)
        date_strs.append(date_str)
        chunk_dfs.append(df)
        chunk_row_cnt += df.shape[0]
        ret_stats.inc_raw(file_cnt=1,
                          bytes_read=path.lstat().st_size,
                          row_cnt=df.shape[0])

        if chunk_row_cnt > rows_per_chunk or (limit is not None and i >= limit):
            chunk_df_out: DataFrame = pd.concat(chunk_dfs)
            assert isinstance(chunk_df_out, DataFrame)
            fname_out = f"{min(date_strs)}-{max(date_strs)}.{type_suffix}{sampled_suffix}.parquet"
            chunk_path_out = dst_dir_path / fname_out
            if verbose > 0:
                L.info("Saving parquet chunk: %s", chunk_path_out)
            if verbose > 1:
                ret_stats.log()
            chunk_df_out.to_parquet(chunk_path_out)
            ret_stats.inc_parquet(file_cnt=1,
                                  bytes_written=chunk_path_out.lstat().st_size,
                                  row_cnt=chunk_df_out.shape[0])
            chunk_dfs = []
            date_strs = []
            chunk_row_cnt = 0
            chunk_idx += 1

        if limit is not None and i >= limit:
            break

    return ret_stats


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
        raw_fpaths: Optional list of paths to raw data files.
        verbose (int): Verbosity level

    Returns:
    -------
    A tuple containing a DataFrame with concatenated data and a list of file names.

    """
    # %%
    schema_df = load_schema(typ)
    suffix = "export" if typ == "events" else typ

    glob_patterns = [f'*.{suffix}.CSV.zip', f'*.{suffix}.CSV.sampled_*.*.zip']
    raw_fpaths = sorted(list(src_path.glob(glob_patterns[0]))
                        + list(src_path.glob(glob_patterns[1])))
    if len(raw_fpaths) == 0:
        L.warning("raw_paths is empty, src_path='%s', glob_patterns=%s",
                  src_path, glob_patterns)
    # %%

    col_names, dtype_map = get_cols_and_types(schema_df, column_name_mode)

    for fpath in raw_fpaths:
        try:
            if fpath.lstat().st_size == 0:
                L.info(f"WARN: Skipping empty file: {fpath}")
                continue
            interval_df = pd.read_csv(fpath, sep='\t', names=col_names, dtype=dtype_map)
        except zipfile.BadZipFile:
            L.info(f"BadZipFile exception for {fpath} (size={fpath.lstat().st_size})")
            continue
        except ValueError as err:
            if err.args[0].startswith('Zero files found'):
                L.warning("Zero files found in fpath: %s, skipping", fpath)
                continue
        except Exception as exc:
            L.error(f"Exception reading parquet from path: %s\n%s", fpath, exc.args)
            raise exc

        if verbose > 1:
            L.info(f'fname: {fpath.name} - {interval_df.shape[0]} records')

        yield interval_df, fpath

def interpret_fname(path: Path) -> tuple[str, str]:
    """Extract date_str and sample suffix from filename"""
    fname = path.name
    date_str = fname.split('.')[0]
    mch = re.search(f'(\.sampled_[0-9.]+)\.zip', fname)
    if mch:
        return date_str, mch.group(1)
    else:
        return date_str, ""

def get_cols_and_types(schema_df: DataFrame,
                       col_name_mode: ColNameMode) -> tuple[list[str], dict[str, dtype]]:
    """Extract column names and their corresponding data types from a schema DataFrame.

    Args:
    ----
        schema_df (DataFrame): The DataFrame containing schema information.
        rename_cols (bool): A flag indicating whether to use snake_case column names.

    Returns:
    -------
        tuple[list[str], dict[str, type]]: A tuple containing a list of column names
        and a dictionary mapping column names to their respective data types.

    """
    col_names: list[str] = list(schema_df['snake_col_name']
                                if col_name_mode == 'snake_case'
                                else schema_df['column'])

    col_2_type_desc: dict[str, str] = dict(zip(col_names, schema_df["pandas_type"], strict=True))
    dtype_map = {col: TYPE_DESC_TO_NP_TYPE[type_desc]
                 for col, type_desc in col_2_type_desc.items()}

    return col_names, dtype_map


GDELT2_TYPE_DESC_MAPPING = {
    "INTEGER": "int64",
    "STRING": "str",
    "HALF": "float16",
    "FLOAT": "float32",
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
TYP_TO_SCHEMA_PATH: dict[GdeltV1Type, str] = {
    "events": "docs/schema_files/GDELT_v1.events.columns.csv",
    "gkgcounts": "docs/schema_files/GDELT_v1.gkgcounts.columns.csv"
}


def load_schema(typ: GdeltV1Type) -> DataFrame:
    """Load the schema for GDELT 1.0 data based on the specified type.

    Args:
    ----
        typ (Gdelt2FileType): The type of GDELT data to load the schema for.

    Returns:
    -------
        DataFrame: The schema DataFrame with renamed columns and mapped data types.

    """
    local_path = TYP_TO_SCHEMA_PATH[typ]
    schema_df = pd.read_csv(local_path)

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
