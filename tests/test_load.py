from collections import Counter
from pandas import Series

import data_proc.load as ld
from test.utils import _read_one_type


# %%

def _interactive_testing():
    # %%
    from importlib import reload
    from shared import runpyfile
    reload(ld)
    runpyfile("test/test_load.py")
    # %%


def test_proc_one_events() -> None:
    """Test proc_one function"""
    interval_df = _read_one_type("events")

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
    data_df = _read_one_type("gkg", do_massage=True)

    type_cnts = get_type_cnts(data_df['pub_date'])
    assert set(type_cnts.keys()) == {"date"}
    # %%

def get_type_cnts(series: Series) -> dict[str, int]:
    """Return a dictionary of the forma {"type_name": cnt} for all distinct types in Series"""
    return series.apply(lambda v: type(v).__name__).value_counts().to_dict()


def test_proc_one_gkgcounts() -> None:
    """Test proc_one function"""
    interval_df = _read_one_type("gkgcounts")
    n_source_urls = interval_df['source_urls'].str.len()
    assert len(n_source_urls.value_counts()) > 1, "Splitting of source urls failed" # noqa: S101

    n_sources = interval_df['sources'].str.len()
    print(n_sources.value_counts()), "Splitting of source urls failed"
    cntr: Counter[str] = Counter()
    def one_update(ev_ids: list[int] | None) -> None:
        if ev_ids is not None:
            cntr.update(type(ev_id).__name__ for ev_id in ev_ids)

    interval_df['event_ids'].apply(one_update)

    assert set(cntr.keys()).issubset(['None', 'int'])  # noqa: S101
