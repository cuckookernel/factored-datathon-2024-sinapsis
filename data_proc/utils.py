"""Utilties for data processing"""
from collections.abc import Generator, Iterable, Mapping
from typing import TypeVar

from data_proc.common import NaN
from shared import Optional, Path, logging, np, re

L = logging.getLogger("dp-util")
# %%

def ensure_float(a_str: str) -> float:
    """Convert to float attempting cleanup, return NaN if cleanup fails"""
    if a_str == '':
        return NaN
    try:
        return float(a_str)
    except ValueError:
        L.info("a_str=%s is not a float, attempting cleanup", a_str)
        a_clean = re.sub("[^0-9-.]", "", a_str)
        try:
            return float(a_clean)
        except ValueError:
            L.info("a_str=%s is not a float after cleanup", a_clean)
            return NaN


# %%
def try_into_int64(a_str: str) -> Optional[np.int64]:
    """Convert to float attempting cleanup, return NaN if cleanup fails"""
    if a_str == '':
        return None

    try:
        return np.int64(a_str)
    except OverflowError:
        L.info("a_str='%s'  caused overflow, attempting cleanup", a_str)
        return None
# %%


def interpret_fname(path: Path) -> tuple[str, str]:
    """Extract date_str and sample suffix from filename"""
    fname = path.name
    date_str = fname.split('.')[0]
    mch = re.search(r'(\.sampled_[0-9.]+)\.zip', fname)
    if mch:
        return date_str, mch.group(1)
    else:
        return date_str, ""



def rename_col(col: str, col_renames: Mapping[str, str]) -> str:
    """Rename a column based on a mapping or convert it to snake_case if not found.

    Args:
    ----
    col : str
        The column name to be renamed.
    col_renames : Mapping[str, str]
        A mapping of original column names to new column names.

    Returns:
    -------
    str
        The renamed column name or the column name converted to snake_case if
        not found in col_renames.

    """
    if col in col_renames:
        return col_renames[col]
    else:
        return camel_to_snake_case(col)

def camel_to_snake_case(identifier: str) -> str:
    """Convert an identifier from camelCase to snake_case

    Args:
    ----
    identifier (str): The identifier to transform

    Examples:
    --------
        camel_to_snake_case("MonthYear") => "month_year"
        camel_to_snake_case("ActionGeo_Lat") => "action_geo_lat"
        camel_to_snake_case("Actor2Religion1Code") => "actor2_religion1_code"

    """
    step1 = re.sub(r'ID', 'Id', identifier)
    step1b = re.sub(r'ADM', 'Adm', step1)
    step2 = re.sub(r'(?<!^)(?=[A-Z])', '_', step1b).lower()

    return re.sub(r'_{2,}', '_', step2)


def try_to_int(a: str, default: int = 1) -> int:
    """Try converting `a` to int. On failure return `default`"""
    try:
        return int(a)
    except ValueError:
        L.info("Invalid value for int conversion")
        return default


T_ = TypeVar("T_")

def try_int_or(a:str, default: T_) -> int | T_:
    """Try converting `a` to int. On failure return `default`"""
    try:
        return int(a)
    except ValueError:
        return default


def batches_from_iter(an_iterable: Iterable[T_], batch_size: int) \
        -> Generator[list[T_], None, None]:
    """Produce batches of batch_size of from an iter, until exausting it.

    Note: last batch could have a smaller size
    """
    batch: list[T_] = []
    for item in an_iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
        else:
            continue

    if len(batch) > 0:
        yield batch
