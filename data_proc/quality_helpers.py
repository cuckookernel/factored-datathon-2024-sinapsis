"""Diagnosing quality problems in raw data"""
import logging
from pathlib import Path

import pandas as pd
from numpy import dtype

L = logging.getLogger("dp-qh")

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
