"""Logging et al."""

# disabling ruff F401:
#   External classes and functions not used in this script might be imported from other modules
import logging
import os
import re  # noqa: F401
from collections.abc import Callable, Mapping  # noqa: F401
from datetime import date, time, timedelta  # noqa: F401
from pathlib import Path  # noqa: F401
from typing import Optional  # noqa: F401

import numpy as np  # noqa: F401
import pandas as pd  # noqa: F401
from dotenv import load_dotenv
from pandas import DataFrame, Series, Timedelta, Timestamp  # noqa: F401
from rich.logging import RichHandler

# common classes imported from stdlibrary / pandas / numpy, so that we can import everything
# from this module and have a more pleasant life:

FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", format=FORMAT, datefmt="[%X]",
    handlers=[RichHandler(locals_max_length=120)],
)  # set level=20 or logging.INFO to turn off debug
logger = logging.getLogger("rich")

logger.info("rich logger initialized")


def read_env() -> None:
    """Only use when testing code interactively on the console

    DO NOT USE in prod!
    """
    load_dotenv(f"{os.environ['HOME']}/profile.env")


def runpyfile(path: str) -> None:
    """Run python file interactively using the associated IPython(?) function..."""
    # noinspection PyUnresolvedReferences
    runfile(path)  # noqa: F821  # type: ignore [name-defined]
