"""Utilities to load data from disk into Dataframes  and set the schema on those"""
# %%
import os
import re
import zipfile
from collections.abc import Callable, Generator, Mapping
from pathlib import Path
from typing import Literal, Optional, TypeAlias

import numpy as np
import pandas as pd
from numpy import dtype
from pandas import DataFrame

from data_proc.common import GdeltV1Type, NaN, gdelt_base_data_path
from shared import logging, runpyfile

L = logging.getLogger("load")

ColNameMode: TypeAlias = Literal["orig", "snake_case"]
# %%

def _interactive_run_load() -> None:
    # %%
    runpyfile("data_proc/load.py")
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
    print(rows_per_chunk)
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
    # save_parquet_chunks(typ, rows_per_chunk, src_path / 'raw_data', limit=100,
    #                    verbose=1)
    # %%
    for col in one_df.columns:
        cnt = (one_df[col] == 'GOV').sum()

        if cnt > 0:
            print(f"{col}: {cnt}")
    # %%


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
        err_msg = f"raw_paths is empty, src_path={src_path}, glob_patterns={glob_patterns}"
        L.error("{}", err_msg)
        raise RuntimeError(err_msg)
    # %%

    col_names, dtype_map, converters = get_cols_and_types(schema_df, column_name_mode)

    L.info("col_names=%s, converters=%s", col_names, converters)

    for fpath in raw_fpaths:
        try:
            if fpath.lstat().st_size == 0:
                L.info(f"WARN: Skipping empty file: {fpath}")
                continue
            header=None if typ == 'events' else 1
            interval_df = pd.read_csv(fpath, sep='\t', names=col_names,
                                      dtype=dtype_map, header=header, converters=converters)

        except zipfile.BadZipFile:
            L.info(f"BadZipFile exception for {fpath} (size={fpath.lstat().st_size})")
            continue
        except (ValueError, OverflowError) as err:
            if err.args[0].startswith('Zero files found'):
                L.warning("Zero files found in fpath: %s, skipping", fpath)
                continue
            else:
                L.error(f"When reading: {fpath}\nerror msg: {err.args[0]}")
                diagnose_problem(fpath, col_names)
                find_faulty_row(fpath, col_names, dtype_map)
                raise
        except Exception as exc:
            L.error("Exception reading parquet from path: %s\n%s", fpath, exc.args)
            raise

        if verbose > 1:
            L.info(f'fname: {fpath.name} - {interval_df.shape[0]} records')

        yield interval_df, fpath


def ensure_float(a_str: str) -> float:
    """Convert to float attempting cleanup, return NaN if cleanup fails"""
    if a_str == '':
        return NaN
    try:
        return float(a_str)
    except ValueError:
        L.info("a_str=%s is not a float, attempting cleanup", a_str)
        a_clean = re.sub("[^0-9-.]", "", a_str)
        try:
            return float(a_clean)
        except ValueError:
            L.info("a_str=%s is not a float after cleanup", a_clean)
            return NaN

def try_into_int64(a_str: str) -> Optional[np.int64]:
    """Convert to float attempting cleanup, return NaN if cleanup fails"""
    if a_str == '':
        return None

    try:
        return np.int64(a_str)
    except OverflowError:
        L.info("a_str='%s'  caused overflow, attempting cleanup", a_str)
        return None

def diagnose_problem(fpath: Path, col_names: list[str]) -> None:
    """Diagnose what might be wrong with raw csv file, called in case of failure"""
    n_cols = len(col_names)
    L.info(f"col_names has: {n_cols}: {col_names}")
    data_df = pd.read_csv(fpath, sep="\t")
    n_cols_file = data_df.shape[1]
    L.info(f"file has: {n_cols_file} columns")
    first_row = data_df.iloc[0]
    L.info(f"first row: {list(first_row)}")

    if n_cols == n_cols_file:
        data_df = pd.read_csv(fpath, names=col_names, sep="\t")
        L.info("Num cols ok. Row 1:\n%s", data_df.iloc[1])

def find_faulty_row(fpath: Path, col_names: list[str], dtype_map: dict[str, dtype]) -> None:
    """Apply bisection to identify faulty raw"""
    raw_df = pd.read_csv(fpath, sep="\t")
    print(f"raw_df: {raw_df.shape}")

    start = 1
    end = raw_df.shape[0]

    # Run bisection to find problematic row
    while (end - start) > 1:
        mid = (start + end) // 2
        try:
            pd.read_csv(fpath, sep="\t", names=col_names, dtype=dtype_map,
                         header=start, nrows=mid-start)
        except (OverflowError, ValueError) as err:
            end = mid
            print(f"first_half faulty, new bracket  start: {start} end:{end} err: {err.args[0]}")
            continue

        try:
            pd.read_csv(fpath, sep="\t", names=col_names, dtype=dtype_map,
                        header=mid, nrows=end-mid)
        except (OverflowError, ValueError) as err:
            start = mid
            print(f"first_half faulty, new bracket  start: {start} end:{end}  err: {err.args[0]}")

    bad_part = pd.read_csv(fpath, sep="\t", names=col_names, header=start, nrows=end-start)

    print("\n\nBad Rows:")
    for _, row in bad_part.iterrows():
        print(row)


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
                       col_name_mode: ColNameMode,
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

    if "reported_count" in col_names:
        converters["reported_count"] = try_into_int64 # type: ignore [assignment]
        del dtype_map["reported_count"]

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
    """Return local schema path for a given GDELT file type"""
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
