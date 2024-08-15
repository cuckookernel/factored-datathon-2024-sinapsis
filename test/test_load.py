from collections import Counter
from pandas import DataFrame

from data_proc.common import GdeltV1Type, gdelt_base_data_path, ColNameMode
from data_proc.load import MASSAGE_FUN_FOR_TYP, proc_one
from data_proc.schema_helpers import load_schema, get_cols_and_types


def test_proc_one() -> None:
    """Test proc_one function"""
    # %%
    typ: GdeltV1Type = "gkgcounts"
    src_path = gdelt_base_data_path() / f'last_1y_{typ}' / 'raw_data'
    fpath = next(iter(src_path.glob("*.zip")))

    schema_df = load_schema(typ=typ)
    column_name_mode: ColNameMode = "snake_case"
    massaging_fun = MASSAGE_FUN_FOR_TYP.get(typ)

    schema_traits = get_cols_and_types(schema_df, column_name_mode)

    interval_df = proc_one(fpath, typ, schema_traits, massaging_fun)
    assert isinstance(interval_df, DataFrame)  # noqa: S101 # this is a test; assert is ok

    # %%
    n_source_urls = interval_df['source_urls'].str.len()
    assert len(n_source_urls.value_counts()) > 1, "Splitting of source urls failed" # noqa: S101

    n_sources = interval_df['sources'].str.len()
    print(n_sources.value_counts()), "Splitting of source urls failed"
    # %%
    cntr: Counter[str] = Counter()
    def one_update(ev_ids: list[int] | None) -> None:
        if ev_ids is not None:
            cntr.update(type(ev_id).__name__ for ev_id in ev_ids)

    interval_df['event_ids'].apply(one_update)

    assert set(cntr.keys()).issubset(['None', 'int'])  # noqa: S101
    # %%
