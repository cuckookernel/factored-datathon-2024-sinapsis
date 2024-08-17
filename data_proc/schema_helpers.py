"""Dealing with raw data schemas"""
from dataclasses import dataclass

from numpy import dtype

from data_proc.common import ColNameMode, GdeltV1Type
from data_proc.utils import ensure_float, rename_col
from shared import Callable, DataFrame, Path, np, pd


@dataclass
class SchemaTraits:
    """Column names, types, converters"""

    col_names: list[str]
    dtype_map: dict[str, dtype]
    converters: dict[str, Callable]



def get_cols_and_types(schema_df: DataFrame,
                       col_name_mode: ColNameMode) -> SchemaTraits:
    """Extract column names and their corresponding data types from a schema DataFrame.

    Args:
    ----
        schema_df (DataFrame): The DataFrame containing schema information.
        col_name_mode (ColNameMode): A flag indicating whether to use original or
            snake_case column names.

    Returns:
    -------
        tuple[list[str], dict[str, type]]: A tuple containing a list of column names
        and a dictionary mapping column names to their respective data types.

    """
    col_names: list[str] = list(schema_df['snake_col_name']
                                if col_name_mode == 'snake_case'
                                else schema_df['column'])

    col_2_type_desc: dict[str, str] = dict(zip(col_names, schema_df["pandas_type"], strict=True))
    dtype_map = {col: TYPE_DESC_TO_NP_TYPE[pandas_type_desc]
                 for col, pandas_type_desc in col_2_type_desc.items()
                 if not pandas_type_desc.startswith('float')}

    converters = {
        col: ensure_float
        for col, pandas_type_desc in col_2_type_desc.items()
        if pandas_type_desc.startswith('float')
    }

    return SchemaTraits(col_names, dtype_map, converters)


GDELT2_TYPE_DESC_MAPPING = {
    "INTEGER": "int64",
    "STRING": "str",
    "HALF": "float16",
    "FLOAT": "float64",
    "DOUBLE": "float64",
}


TYPE_DESC_TO_NP_TYPE: dict[str, dtype] = {
    "int64": np.dtype('int64'),
    "str": np.dtype(str),
    "float16": np.dtype('float16'),
    "float32": np.dtype('float32'),
    "float64": np.dtype('float64'),
}

# %%
def schema_path(typ: GdeltV1Type) -> Path:
    """Return local schema path for a given GDELT file type"""
    return Path(f"docs/schema_files/GDELT_v1.{typ}.columns.csv")


def load_schema(typ: GdeltV1Type) -> DataFrame:
    """Load the schema for GDELT 1.0 data based on the specified type.

    Args:
    ----
        typ (Gdelt2FileType): The type of GDELT data to load the schema for.

    Returns:
    -------
        DataFrame: The schema DataFrame with renamed columns and mapped data types.

    """
    # %%
    local_path = schema_path(typ)
    schema_df = pd.read_csv(local_path)
    # %%

    if 'col_renamed' in schema_df:
        schema_df['snake_col_name'] = schema_df['col_renamed']
    else:
        col_renames = {
            "GLOBALEVENTID": "ev_id",
            "SQLDATE": "date_int",
            "MONTH_YEAR": "date_int",
            "DATEADDED": "date_added",
            "SOURCEURL": "source_url",
        }

        schema_df['snake_col_name'] = (schema_df['column']
            .apply(lambda col: rename_col(col, col_renames))
        )

    schema_df['pandas_type'] = (
        schema_df['data_type'].apply(lambda data_type: GDELT2_TYPE_DESC_MAPPING[data_type])
    )

    return schema_df
