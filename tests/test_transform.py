"""Test transformation functions"""
from collections import Counter

import pandas as pd
from pandas import Series

import data_proc.data_dicts as dd
import data_proc.transform as tr
from data_proc.common import gdelt_base_data_path
from tests.utils import read_one_type

# %%


def _interactive_testing() -> None:
    # %%
    dd.dictionaries_standardization()
    # %%
    pd.set_option('display.max_rows', 100)
    # %%
    name = 'cameo_event'
    pq_fpath = gdelt_base_data_path() / f"{name}/{name}.parquet"
    ce_df = pd.read_parquet(pq_fpath)
    print(ce_df.shape)
    # %%

def test_transform_events() -> None:
    # %%
    events_df = read_one_type("events", do_massage=False)
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


def test_enrich_gkgcounts() -> None:
    # %%
    counts_df = read_one_type("gkgcounts", do_massage=True)
    # %%
    enricher = tr.CodeDescEnricher()
    # %%
    enricher.enrich_ip(counts_df, "geo_country_code", "fips_country")
    enricher.enrich_ip(counts_df, "geo_type", "geo_type")
    # %%
    assert "geo_country_name" in counts_df
    assert "geo_type_desc" in counts_df
    # %%



def test_proc_one_events() -> None:
    """Test proc_one function"""
    interval_df = read_one_type("events")

    ev_date_type_cnts = get_type_cnts(interval_df['ev_date'])
    assert set(ev_date_type_cnts.keys()) == { "date" }

    date_added_type_cnts = get_type_cnts(interval_df['date_added'])
    assert set(date_added_type_cnts.keys()) == {"date"}

    ev_id_type_cnts = get_type_cnts(interval_df['ev_id'])
    assert set(ev_id_type_cnts.keys()) == {"int"}

    ev_id_type_cnts = get_type_cnts(interval_df['ev_id'])
    assert set(ev_id_type_cnts.keys()) == {"int"}

    type_cnts = get_type_cnts(interval_df['ev_code'])
    assert set(type_cnts.keys()) == {"str"}

    type_cnts = get_type_cnts(interval_df['ev_root_code'])
    assert set(type_cnts.keys()) == {"str"}

    type_cnts = get_type_cnts(interval_df['ev_base_code'])
    assert set(type_cnts.keys()) == {"str"}
    # %%


def test_proc_gkg() -> None:
    """Test proc_one function"""
    # %%
    data_df = read_one_type("gkg", do_massage=True)

    type_cnts = get_type_cnts(data_df['pub_date'])
    assert set(type_cnts.keys()) == {"date"}
    # %%

def get_type_cnts(series: Series) -> dict[str, int]:
    """Return a dictionary of the forma {"type_name": cnt} for all distinct types in Series"""
    return series.apply(lambda v: type(v).__name__).value_counts().to_dict()


def test_proc_one_gkgcounts() -> None:
    """Test proc_one function"""
    gkgc = read_one_type("gkgcounts")
    n_source_urls = gkgc['source_urls'].str.len()
    assert len(n_source_urls.value_counts()) > 1, "Splitting of source urls failed"

    n_sources = gkgc['sources'].str.len()
    print(n_sources.value_counts()), "Splitting of source urls failed"
    cntr: Counter[str] = Counter()
    def one_update(ev_ids: list[int] | None) -> None:
        if ev_ids is not None:
            cntr.update(type(ev_id).__name__ for ev_id in ev_ids)

    gkgc['event_ids'].apply(one_update)

    assert set(cntr.keys()).issubset(['None', 'int'])
