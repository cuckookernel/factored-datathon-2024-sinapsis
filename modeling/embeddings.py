"""Embedder classes based on Sentence Transformers"""
from collections.abc import Sequence
from pathlib import Path
from typing import Optional

import joblib  # type: ignore # noqa: PGH003
import numpy as np
from pandas import Series
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import PCA  # type: ignore # noqa: PGH003
from torch import Tensor
from typing_extensions import Self

from shared import NpArray, assert_type, logging

L = logging.getLogger("embed")
# %%

class ByCacheEmbedder:
    """Embedder of values using pre-cached embeddings"""

    def __init__(self, *, raw_values: Sequence[str], embeddings: np.ndarray) -> None:
        self.embed_dim = embeddings.shape[1]
        if len(raw_values) != embeddings.shape[0]:
            raise ValueError("Expecting embeddings to have {len(raw_values)} rows, "
                             f"but has {embeddings.shape[0]}")
        self.cache = dict(zip(raw_values, embeddings, strict=False))
        self.default = np.zeros((self.embed_dim,), dtype=embeddings.dtype)

    def transform(self, raw_values: Sequence[str]) -> Sequence[np.ndarray]:
        """Produce embeddings from raw_values"""
        ret = [ self.default for _ in range(len(raw_values)) ]

        for i, value in enumerate(raw_values):
            if embed := self.cache.get(value):
                ret[i] = embed
            else:
                L.error("raw_value: `{value}` not found in cache. This should not happen!")

        return ret
# %%



class ByCacheAndBackupEmbedder:
    """Embedder of values using pre-cached embeddings"""

    def __init__(self, *, raw_values: Sequence[str] | Series,
                 reduced_embeddings: NpArray | Tensor,
                 pca: PCA,
                 bkup_transformer_name: str) -> None:
        self.embed_dim = reduced_embeddings.shape[1]
        if len(raw_values) != reduced_embeddings.shape[0]:
            raise ValueError(f"Expecting embeddings to have {len(raw_values)} rows, "
                             f"but has {reduced_embeddings.shape[0]}")
        self.cache = dict(zip(raw_values, reduced_embeddings, strict=False))
        if pca.n_components != self.embed_dim:
            raise ValueError(f"Expected pca.n_components to be {self.embed_dim} but "
                             f"it is {pca.n_components}")
        self.pca = pca
        self.bkup_transformer_name = bkup_transformer_name
        self.bkup_transformer: Optional[SentenceTransformer] = None

    def transform(self, raw_values: Sequence[str] | Series) -> Sequence[NpArray]:
        """PRoduce embeddings from raw_values"""
        ret: list[np.ndarray] = [ None ] * len(raw_values) # type: ignore [list-item]

        pending_idxs: list[int] = []
        for i, value in enumerate(raw_values):
            embed = self.cache.get(value)
            if  embed is not None:
                ret[i] = embed
            else:
                pending_idxs.append(i)

        more_embeds = assert_type(self.bkup_transformer, SentenceTransformer,
                                  ).encode([raw_values[i] for i in pending_idxs])
        more_embeds_reduced = self.pca.transform(more_embeds)

        for i, embed in zip(pending_idxs, more_embeds_reduced, strict=False):
            ret[i] = embed

        return ret

    def persist(self, file_path: Path) -> None:
        """Persist via joblib but avoid storing the whole transformer model with it"""
        tmp = self.bkup_transformer
        self.bkup_transformer = None
        joblib.dump(self, file_path)
        L.info(f"Dumped {type(self).__name__} to {file_path} ({file_path.lstat().st_size} bytes)")
        self.bkup_transformer = tmp

    @classmethod
    def restore_from_file(cls, file_path: Path) -> Self:
        """Restore from file and populate via joblib ang populate cls.bkup_tranformer field"""
        L.info(f"Loading {cls.__name__} from {file_path} ({file_path.lstat().st_size} bytes)")
        obj = joblib.load(file_path)
        obj.bkup_transformer = SentenceTransformer(obj.bkup_transformer_name)

        return obj
# %%
