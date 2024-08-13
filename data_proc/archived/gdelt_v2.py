"""Code that work for gdelt_v2"""

import random
from datetime import date
from datetime import timedelta
from pathlib import Path

import requests

BASE_URL = "http://data.gdeltproject.org/gdeltv2"

def timestamps_list_gdelt_v2(start: date, end: date,
                    sample_fraction: float = 1.0,
                    random_seed: int = 0) -> list[str]:
    """Generate a list of timestamps between the start and end dates with 15-minute intervals.

    Args:
    ----
    start (Date): The starting date.
    end (Date): The ending date.
    sample_fraction (float, optional): The sampling fraction
    random_seed (int, optional, default=0): Control exactly what sample to get.

    Returns:
    -------
    list[str]: A list of timestamps in the format 'YYYYMMDDHHMM00'.

    """
    ret: list[str] = []
    date_cursor = start

    random.seed(random_seed)

    while date_cursor < end:
        date_str = date_cursor.strftime("%Y%m%d")
        for hour in range(24):
            ret.extend(f"{date_str}{hour:02d}{mins:02d}00"
                       for mins in [0, 15, 30, 45]
                       if random.random() < sample_fraction # noqa: S311
                       )

        date_cursor += timedelta(1.0)

    return ret


GDELT_SCHEMA_BASE = "https://raw.githubusercontent.com/linwoodc3/gdelt2HeaderRows/master/schema_csvs"

GDELT_SCHEMA_URLS = {
    "events": GDELT_SCHEMA_BASE + "/GDELT_2.0_Events_Column_Labels_Header_Row_Sep2016.csv",
    "mentions": GDELT_SCHEMA_BASE + "/GDELT_2.0_eventMentions_Column_Labels_Header_Row_Sep2016.tsv",
    "gkg": GDELT_SCHEMA_BASE + "/GDELT_2.0_gdeltKnowledgeGraph_Column_Labels_Header_Row_Sep2016.tsv",  # noqa: E501
}


def download_gdelt_schema_files() -> dict[str, Path]:
    """Download GDELT schema files from the provided URLs and save them locally in the 'docs/schema_files' directory.

    This function iterates through the GDELT schema URLs, downloads each file if it doesn't already exist locally,
    and saves it in the specified directory.

    Returns
    -------
       dict[str, Path]  a dictionary of the form {"events": ..., "mentions": ...} containing local paths

    """# noqa: E501
    local_schema_paths: dict[str, Path] = {}
    for gdelt_type, url in GDELT_SCHEMA_URLS.items():
        fname = url.split('/')[-1]
        local_path = Path("./docs/schema_files") / fname
        local_schema_paths[gdelt_type]=local_path

        if local_path.exists():
            # file already donwloaded, skipping
            pass

        # download the file
        response = requests.get(url, timeout=30)
        with local_path.open('wb') as f_out:
            f_out.write(response.content)

    return local_schema_paths
