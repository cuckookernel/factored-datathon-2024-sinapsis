import csv
from pathlib import Path

import pandas as pd
from shared import logging
from data_proc.common import gdelt_base_data_path

L = logging.getLogger("d_dict")

CONFIG = {
    "gdelt_quad_classes": {
        "url": "docs/data_dictionaries/GDELT.quadclasses.txt",
        "cols": ["QUADCLASS", "DESCRIPTION"],
        "sep": ",",
        "types": ["int", "str"],
        "cols_renamed": ["quad_class", "ev_desc"],
    },
    "cameo_event_codes": {
        "url": "https://www.gdeltproject.org/data/lookups/CAMEO.eventcodes.txt",
        "cols": ["CAMEOEVENTCODE", "EVENTDESCRIPTION"],
        "cols_renamed": ["ev_code", "ev_desc"],
    },
    "cameo_actor_type": {
        "url": "https://www.gdeltproject.org/data/lookups/CAMEO.type.txt",
        "cols": ["CODE", "LABEL"],
        "cols_renamed": ["a_type_code", "a_type_desc"],

    },
    "fips_country_codes": {
        "url": "https://www.gdeltproject.org/data/lookups/FIPS.country.txt",
        "cols_renamed": ["fips_country_code", "country_name"],
        "types": ["str", "str"],
    },
    "cameo_country": {
        "url": "https://www.gdeltproject.org/data/lookups/CAMEO.country.txt",
        "cols": ["CODE", "LABEL"],
        "cols_renamed": ["cameo_country_code", "country_name"],
    },
    "geo_type": {
        "url": "docs/data_dictionaries/geo_types.txt",
        "sep": ",",
        "cols": ["GEO_TYPE", "DESCRIPTION"],
        "cols_renamed": ["geo_type", "geo_type_desc"],
        "types": ["int", "str"],
    }
}


def dictionaries_standardization():
    """Transform CAMEO event codes.txt to a more standard csv and parquet"""
    for name, spec in CONFIG.items():

        url = spec["url"]
        cols = spec.get("cols")
        cols_renamed = spec.get("cols_renamed")
        header = 0 if cols is not None else None
        types = spec.get("types", ["str"] * 2)

        sep = spec.get("sep", "\t")
        dtype_map = dict(zip(cols_renamed, types))

        print("name")
        L.info(f"Loading dictionary - {name}, from: {url}")
        L.info(f"header={header} sep={sep!r} dtype_map={dtype_map}")

        dict_df = pd.read_csv(url, sep=sep, dtype=dtype_map, header=header)
        if header is not None:
            df_cols = list(dict_df.columns)
            assert df_cols == cols, f"df_cols={df_cols}, expected: {cols}"

        dict_df.columns = spec["cols_renamed"]
        L.info("First rows %s", dict_df.head())

        # Save csv
        csv_path = Path(f"docs/data_dictionaries/{name}.std.csv")
        L.info(f"Saving standardized csv to: {csv_path!r}")
        dict_df.to_csv(csv_path, quoting=csv.QUOTE_ALL, index=False)

        # save parqet
        pq_fpath = gdelt_base_data_path() / f"{name}/{name}.parquet"
        pq_fpath.parent.mkdir(parents=True, exist_ok=True)
        L.info(f"Saving parquet  to: {pq_fpath!r}")
        dict_df.to_parquet(pq_fpath)
    # %%
