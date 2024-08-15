"""Utilities to load data from disk into Dataframes  and set the schema on those"""
# %%
import os
import zipfile
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Optional

import pandas as pd
from numpy import isnan
from pandas import DataFrame

from data_proc.schema_helpers import get_cols_and_types, load_schema
from data_proc.common import GdeltV1Type, ColNameMode, gdelt_base_data_path
from data_proc.quality_helpers import find_faulty_row, diagnose_problem
from data_proc.utils import try_to_int
from shared import logging, runpyfile

L = logging.getLogger("load")

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
        L.error("%s", err_msg)
        raise RuntimeError(err_msg)

    L.info(f"{len(raw_fpaths)} found - first={raw_fpaths[0]} - last={raw_fpaths[-1]}")
    # %%

    col_names, dtype_map, converters = get_cols_and_types(schema_df, column_name_mode)

    L.info("col_names=%s", col_names)
    L.info("dtype_map=%s", dtype_map)
    L.info("converters=%s", converters)

    massaging_fun = MASSAGE_FUN_FOR_TYP.get(typ)

    for fpath in raw_fpaths:
        try:
            if fpath.lstat().st_size == 0:
                L.info(f"WARN: Skipping empty file: {fpath}")
                continue
            header=None if typ == 'events' else 1
            interval_df = pd.read_csv(fpath, sep='\t', names=col_names,
                                      dtype=dtype_map, header=header, converters=converters)

            if massaging_fun is not None:
                interval_df = massaging_fun(interval_df)

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


def massage_events(data_df: DataFrame) -> DataFrame:
    """Massage the raw gkgcounts data

    This function works only if we are doing column renaming.
    It's not hard to make it more general, but wanted to delay that for now...
    """

    date_str = data_df["date_int"].astype(str)
    data_df["pub_date"] = pd.to_datetime(date_str, format="%Y%m%d").dt.date
    data_df["source_urls"] = data_df["source_urls"].str.split(";")
    data_df["event_ids"] = data_df["event_ids"].apply(get_event_ids_array)

    return data_df

def massage_gkgcounts(data_df: DataFrame) -> DataFrame:
    """Massage the raw gkgcounts data

    This function works only if we are doing column renaming.
    It's not hard to make it more general, but wanted to delay that for now...
    """

    pub_date_str = data_df["pub_date"].astype(str)
    data_df["pub_date"] = pd.to_datetime(pub_date_str, format="%Y%m%d").dt.date
    data_df["source_urls"] = data_df["source_urls"].str.split(";")
    data_df["event_ids"] = data_df["event_ids"].apply(get_event_ids_array)

    return data_df




MASSAGE_FUN_FOR_TYP: dict[GdeltV1Type, Callable[[DataFrame],DataFrame]] = {
    "gkgcounts": massage_gkgcounts
}

def get_event_ids_array(a_str: Optional[str]) -> Optional[list[int]]:
    if a_str is None:
        return None

    if isinstance(a_str, str):
        return [try_to_int(piece, default=-1) for piece in a_str.split(",")]
    elif isinstance(a_str, float) and isnan(a_str):
        return None
    else:
        raise TypeError(f"Invalid type found: {type(a_str)} value: `{repr(a_str)}`")
