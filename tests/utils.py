"""Testing utilities"""
from pandas import DataFrame

from data_proc import load as ld
from data_proc.common import ColNameMode, GdeltV1Type, gdelt_base_data_path
from data_proc.load import MASSAGE_FUN_FOR_TYP
from data_proc.schema_helpers import get_cols_and_types, load_schema


def read_one_type(typ: GdeltV1Type, do_massage: bool = True) -> DataFrame:
    """Read first file of a given type and optionally apply massage"""
    # %%
    src_path = gdelt_base_data_path() / f'last_1y_{typ}' / 'raw_data'
    fpath = next(iter(src_path.glob("*.zip")))

    schema_df = load_schema(typ=typ)
    column_name_mode: ColNameMode = "snake_case"
    massaging_fun = MASSAGE_FUN_FOR_TYP.get(typ) if do_massage else None

    schema_traits = get_cols_and_types(schema_df, column_name_mode)

    data_df = ld.proc_one(fpath, typ, schema_traits, massaging_fun)
    # %%
    assert isinstance(data_df, DataFrame)

    return data_df
