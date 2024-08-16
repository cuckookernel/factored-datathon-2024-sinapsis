from collections import Counter
from pandas import DataFrame, Series

import data_proc.load as ld
from data_proc.common import GdeltV1Type, gdelt_base_data_path, ColNameMode
from data_proc.load import MASSAGE_FUN_FOR_TYP
from data_proc.schema_helpers import load_schema, get_cols_and_types
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

    type_cnts = get_type_cnts(interval_df['pub_date'])
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

# %%

def _read_one_type(typ: GdeltV1Type, do_massage: bool = True) -> DataFrame:
    # %%
    src_path = gdelt_base_data_path() / f'last_1y_{typ}' / 'raw_data'
    fpath = next(iter(src_path.glob("*.zip")))

    schema_df = load_schema(typ=typ)
    column_name_mode: ColNameMode = "snake_case"
    massaging_fun = MASSAGE_FUN_FOR_TYP.get(typ) if do_massage else None

    schema_traits = get_cols_and_types(schema_df, column_name_mode)

    data_df = ld.proc_one(fpath, typ, schema_traits, massaging_fun)
    # %%
    assert isinstance(data_df, DataFrame)  # noqa: S101 # this is a test; assert is ok

    return data_df
