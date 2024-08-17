import pandas as pd

import shared
from data_proc.common import gdelt_base_data_path
from tests.utils import read_one_type

import data_proc.transform as tr
import data_proc.data_dicts as dd
# %%

def _interactive_testing():
    # %%
    reload(shared)
    from importlib import reload
    reload(dd)
    dd.dictionaries_standardization()
    # %%
    pd.set_option('display.max_rows', 100)
    # %%

    all_dicts = dd.load_all_data_dicts()
    # %%
    name = 'cameo_event'
    pq_fpath = gdelt_base_data_path() / f"{name}/{name}.parquet"
    ce_df = pd.read_parquet(pq_fpath)
    # %%

def test_transform_events():
    # %%
    events_df = read_one_type("events", do_massage=False)
    # %%
    reload(tr)
    enricher = tr.CodeDescEnricher()
    # %%
    enricher.enrich_ip(events_df, "ev_code", "cameo_event")
    enricher.enrich_ip(events_df, "ev_base_code", "cameo_event")
    enricher.enrich_ip(events_df, "ev_root_code", "cameo_event")
    enricher.enrich_ip(events_df, "quad_class", "quad_classes")
    # %%
    enricher.enrich_ip(events_df, "a1_type1_code", "cameo_actor_type")
    enricher.enrich_ip(events_df, "a2_type1_code", "cameo_actor_type")
    # %%
