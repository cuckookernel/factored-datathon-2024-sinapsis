"""Logging et al."""

import logging
import os

from dotenv import load_dotenv
from rich.logging import RichHandler

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
