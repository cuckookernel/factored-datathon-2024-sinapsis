"""Data dict standardization etc.."""
import csv
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from pandas import Series

from data_proc.common import gdelt_base_data_path
from shared import assert_type, logging

L = logging.getLogger("d_dict")


@dataclass
class Spec:
    """params for data dictionary loading"""

    url: str
    cols_renamed: list[str]
    cols: list[str] | None = None
    types: list[str] | None = None
    amend: str | None = None
    amend_types: list[str] | None = None
    sep: str | None = None


CONFIG : dict[str, Spec] = {
    "quad_classes": Spec(
        url="docs/data_dictionaries/GDELT.quadclasses.txt",
        cols=["QUADCLASS", "DESCRIPTION"],
        types=["int", "str"],
        sep=",",
        cols_renamed=["quad_class", "ev_desc"],
    ),
    "cameo_event": Spec(
        url="https://www.gdeltproject.org/data/lookups/CAMEO.eventcodes.txt",
        cols=["CAMEOEVENTCODE", "EVENTDESCRIPTION"],
        cols_renamed=["ev_code", "ev_desc"],
        amend="docs/data_dictionaries/CAMEO.eventcodes.amend.csv",
        amend_types=["str", "str"],
    ),
    "cameo_actor_type": Spec(
        url="https://www.gdeltproject.org/data/lookups/CAMEO.type.txt",
        cols=["CODE", "LABEL"],
        cols_renamed=["a_type_code", "a_type_desc"],
    ),
    "fips_country": Spec(
        url="https://www.gdeltproject.org/data/lookups/FIPS.country.txt",
        cols_renamed=["fips_country_code", "country_name"],
        types=["str", "str"],
    ),
    "cameo_country": Spec(
        url="https://www.gdeltproject.org/data/lookups/CAMEO.country.txt",
        cols=["CODE", "LABEL"],
        cols_renamed=["cameo_country_code", "country_name"],
    ),
    "geo_type": Spec(
        url="docs/data_dictionaries/geo_types.txt",
        sep=",",
        cols=["GEO_TYPE", "DESCRIPTION"],
        cols_renamed=["geo_type", "geo_type_desc"],
        types=["int", "str"],
    ),
}
# %%

def load_all_data_dicts() -> dict[str, Series]:
    """Return dict of data dicts of the form {name: series,...}.

    name will range over names
    """
    ret: dict[str, Series] = {}
    for name in CONFIG:
        pq_fpath = gdelt_base_data_path() / f"{name}/{name}.parquet"
        if not pq_fpath.exists():
            L.info("dictionary file not found, running dictionaries_standardization!")
            dictionaries_standardization()

        dict_df = pd.read_parquet(pq_fpath)
        key_col, desc_col = dict_df.columns[0:2]

        ret[name] = pd.Series(dict_df[desc_col].values,
                              dtype="str",
                              index=pd.Index(dict_df[key_col]),
                              name=desc_col.split("_")[-1])

    L.info("Loaded %s data dicts: %r",
           len(ret), {name: len(d) for name, d in ret.items()})

    return ret
# %%

def dictionaries_standardization(names: list[str] | None = None) -> None:
    """Transform CAMEO event codes.txt to a more standard csv and parquet"""
    for name, spec in CONFIG.items():
        if names is not None and name not in names:
            continue

        cols = spec.cols
        sep = spec.sep or "\t"
        header = 0 if cols is not None else None
        types = spec.types or ["str"] * 2

        dtype_map = dict(zip(spec.cols_renamed, types, strict=False))

        L.info(f"Loading dictionary - {name}, from: {spec.url}")
        L.info(f"header={header} sep={sep!r} dtype_map={dtype_map}")

        if header is not None:
            # just check columns first
            tmp_df = pd.read_csv(spec.url, sep=sep, header=header, nrows=1)
            df_cols = list(tmp_df.columns)
            if df_cols != cols:
                raise ValueError(f"df_cols={df_cols}, expected: {cols}")

        dict_df = pd.read_csv(spec.url, sep=sep, names=spec.cols_renamed,
                              dtype=dtype_map, header=header)

        if spec.amend:
            amend_types = assert_type(spec.amend_types, list)
            dtype_map = dict(zip(spec.cols_renamed, amend_types, strict=True))
            amend_df = pd.read_csv(spec.amend, sep=",", quoting=csv.QUOTE_ALL,
                                   dtype=dtype_map)
            if not np.all(amend_df.columns == dict_df.columns):
                raise ValueError("Amend is bad...")
            dict_df = pd.concat([dict_df, amend_df])

        # remove repetitions in codes...
        dict_df = dict_df.groupby(spec.cols_renamed[0]).first().reset_index()

        L.info("First rows %s", dict_df.head())
        # Save csv
        csv_path = Path(f"docs/data_dictionaries/{name}.std.csv")
        L.info(f"Saving standardized csv to: {csv_path!r}")
        dict_df.to_csv(csv_path, quoting=csv.QUOTE_ALL, index=False)

        L.info(f"dict_df.dtypes={dict_df.dtypes}")

        # save parqet
        pq_fpath = gdelt_base_data_path() / f"{name}/{name}.parquet"
        pq_fpath.parent.mkdir(parents=True, exist_ok=True)
        L.info(f"Saving parquet  to: {pq_fpath!r}")
        dict_df.to_parquet(pq_fpath)
    # %%
