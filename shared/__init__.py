"""Logging et al."""

# disabling ruff F401:
#   External classes and functions not used in this script might be imported from other modules
import logging
import os
import re  # noqa: F401
from collections.abc import Callable, Mapping  # noqa: F401
from datetime import date, datetime, time, timedelta  # noqa: F401
from pathlib import Path
from typing import Any, Optional, TypeVar  # noqa: F401

import numpy as np  # noqa: F401
import pandas as pd  # noqa: F401
from dotenv import load_dotenv
from numpy import ndarray as NpArray  # noqa: F401
from pandas import DataFrame, Series, Timedelta, Timestamp  # noqa: F401
from rich.console import Console
from rich.logging import RichHandler

# common classes imported from stdlibrary / pandas / numpy, so that we can import everything
# from this module and have a more pleasant life:

FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", format=FORMAT, datefmt="[%X]",
    handlers=[RichHandler(console=Console(width=150))],
)  # set level=20 or logging.INFO to turn off debug
L = logging.getLogger("rich")

L.info("rich logger initialized")

T_ = TypeVar("T_")

def read_env(file_path: str | Path | None = None) -> None:
    """Only use when testing code interactively on the console

    DO NOT USE in prod!
    """
    if file_path is None:
        file_path = f"{os.environ['HOME']}/profile.env"

    f_path = Path(file_path)
    if f_path.exists():
        load_dotenv(file_path, override=True)
    else:
        L.warning(f"file='{file_path}' does not exist Did nothing...")


def runpyfile(path: str) -> None:
    """Run python file interactively using the associated IPython(?) function..."""
    # noinspection PyUnresolvedReferences
    runfile(path)  # noqa: F821  # type: ignore [name-defined]


def assert_some(val: T_ | None) -> T_:
    """Assert type of something"""
    if val is None:
        raise RuntimeError("Expected not None!")
    else:
        return val


def assert_type(val: object, typ_: type[T_]) -> T_:
    """Assert type of something"""
    assert isinstance(val, typ_), f"Expected type: {typ_}, found: `{type(val)}`, val={val!r}" # noqa: S101
    return val

def assert_type_or_none(val: object, typ_: type[T_]) -> T_ | None:
    """Assert type `val` if not None, otherwiser return None"""
    if val is None:
        return val
    else:
        return assert_type(val, typ_)
