"""Common defs for dealing with data"""
import os
from datetime import timezone
from pathlib import Path
from typing import Literal, Optional

NaN = float("nan")
UTC = timezone.utc

# Type Aliases
GdeltV1Type = Literal["events", "gkg", "gkgcounts"]


def data_path(data_dir: Optional[str] = None) -> Path:
    """Return a Path object representing the data directory.

    Args:
    ----
    data_dir (str, optional): The base data dir directly under which dirs
    such as 'GDELT' are found.

    If data_dir is not provided, the function uses the 'DATA_DIR' environment variable.
    Creates the data directory if it doesn't exist and returns the Path object.

    """
    if data_dir is None:
        data_dir = os.environ['DATA_DIR']

    ret = Path(data_dir)

    if not ret.exists():
        print("Creating data path...")
        ret.mkdir(parents=True)

    return ret


def gdelt_base_data_path(data_dir: Optional[str] = None) -> Path:
    """Return the base path for GDELT data within the specified directory.

    Args:
    ----
        data_dir (Optional[str]): The directory path where the data is stored.

    Returns:
    -------
        Path: The base path for GDELT data within the specified directory.

    """
    dat_path = data_path(data_dir)
    return dat_path / "GDELT"
