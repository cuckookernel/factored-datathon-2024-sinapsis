"""Time series modelling"""
import random

import numpy as np
import pandas as pd
from utilsforecast.plotting import plot_series

from data_proc.common import gdelt_base_data_path

# %%

def _interactive_testing() -> None:
    # %%
    raw_cnts = pd.read_parquet(gdelt_base_data_path() / 'events_agg_country_ev_root.parquet')
    raw_cnts = raw_cnts[raw_cnts['date_added'].astype(str) >= '2023-08-13']

    col_to_predict = "cnt"

    raw_cnts['unique_id'] = (raw_cnts['action_country'] + '-'
                             + raw_cnts['ev_root_code'] + '-'
                             + raw_cnts['ev_root_desc'])

    # raw_cnts['ds'] = (raw_cnts['date_added'] - min_date).apply(lambda td: td.days)
    raw_cnts['ds'] = pd.to_datetime(raw_cnts['date_added'])
    # %%
    if col_to_predict == "cnt":
        raw_cnts["y"] = np.log10(1 + raw_cnts[col_to_predict])
    else:
        raw_cnts["y"] = raw_cnts[col_to_predict]


    data_df = raw_cnts[["unique_id", "ds", "y"]]
    print(data_df.head())
    # %%
    random.seed(2)

    sample_uids = random.choices(raw_cnts['unique_id'], k = 4) # noqa: S311

    fig = plot_series(data_df[data_df['unique_id'].isin(sample_uids)])
    fig.show()
    # %%
