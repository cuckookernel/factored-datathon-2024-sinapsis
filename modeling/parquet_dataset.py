from bisect import bisect
from pathlib import Path
from typing import Optional

import numpy as np
from pyarrow.parquet import ParquetFile # type: ignore # noqa: PGH003
from torch import Tensor
from torch.utils.data import Dataset
from shared import logging

L = logging.getLogger("pq_dset")

class ParquetFilesDataset(Dataset):
    def __init__(self, files_dir: Path,
                 file_name_pattern: str,
                 feature_cols: list[str],
                 target_col: str | None,
                 preload: bool = False,
                 truncate_size: Optional[int] = None) -> None:
        self.file_paths = list(files_dir.glob(file_name_pattern))
        if len(self.file_paths) == 0:
            raise RuntimeError(f"No files matching patern `{file_name_pattern}` under "
                               f"files_dir={files_dir!r}")
        else:
            L.info(f"{len(self.file_paths)} files matching patern `{file_name_pattern}` under "
                   f"files_dir={files_dir!r}")

        self.feature_cols: list[str] = feature_cols
        self.target_col: str | None = target_col

        self.all_columns: list[str]
        if target_col is not None:
            self.all_columns = self.feature_cols + [target_col]
        else:
            self.all_columns = self.feature_cols

        num_rows = []
        for file_path in self.file_paths:
            file = ParquetFile(file_path)
            num_rows.append(file.metadata.num_rows)
            for col in self.feature_cols:
                assert col in file.schema_arrow.names
            if self.target_col is not None:
                assert self.target_col in file.schema_arrow.names
            file.close()

        self.num_rows_cum = np.cumsum([0] + num_rows)
        self.truncate_size = truncate_size
        L.info(f"Total rows={self.num_rows_cum[-1]}")
        self.preloaded_tensors: dict[int, dict[str, Tensor]] = {}
        if preload:
            L.info(f"Preloading {len(self.file_paths)} parquet file")
            for file_idx in range(self._n_files()):
                self._load_file(file_idx)

    def _n_files(self) -> int:
        return len(self.file_paths)

    def __len__(self) -> int:
        if self.truncate_size is not None:
            return self.truncate_size
        else:
            return self.num_rows_cum[-1]

    def _load_file(self, file_idx: int) -> None:
        L.info(f"Loading file={self.file_paths[file_idx].name!r} (file_idx={file_idx})")
        with ParquetFile(self.file_paths[file_idx]) as pq_file:
            contents = pq_file.read(columns=self.all_columns)
            self.preloaded_tensors[file_idx] = {col: Tensor(np.vstack(contents[col].to_pandas()))
                                                for col in self.all_columns}
            pq_file.close()

    def _file_tensor(self, file_idx: int) -> dict[str, Tensor]:
        if file_idx not in self.preloaded_tensors:
            self._load_file(file_idx)

        return self.preloaded_tensors[file_idx]

    def __getitem__(self, item_idx: int) -> dict[str, Tensor]:
        file_index = bisect(self.num_rows_cum, item_idx) - 1

        tensor_dict = self._file_tensor(file_index)
        row_within_file = item_idx - self.num_rows_cum[file_index]

        return {
            col: tensor_dict[col][row_within_file, ...]
            for col in self.all_columns
        }
