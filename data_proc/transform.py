
from shared import logging
from shared import DataFrame, Series
from data_proc.data_dicts import load_all_data_dicts

L = logging.getLogger("transf")

class CodeDescEnricher:
    def __init__(self):
        self.data_dicts: dict[str, Series] = load_all_data_dicts()

    def enrich_ip(self, some_df: DataFrame,
                  code_col: str,
                  data_dict_key: str,
                  tgt_desc_col: str | None = None) -> None:
        """Add a description column in place"""

        data_dict: Series = self.data_dicts[data_dict_key]

        if tgt_desc_col is None:
            if "_code" in code_col:
                tgt_desc_col = code_col.replace("_code", f"_{data_dict.name}")
            else:
                tgt_desc_col = f"{code_col}_desc"
            L.info("Adding column: `%s`, to suppress this message pass an explicity value for "
                   "argument `tgt_desc_col`", tgt_desc_col)

        if tgt_desc_col in some_df:
            L.warning("tgt_desc_col=`%s` already in df, will overwrite", tgt_desc_col)

        # print(f"some_df: {some_df.shape}")
        joined_df = some_df[[code_col]].merge(data_dict, how="left",
                                              left_on=code_col, right_index=True)
        # print(f"joined_df: {joined_df.shape}, {joined_df.columns}")
        desc_col = joined_df.iloc[:, 1]
        some_df[tgt_desc_col] = desc_col


def test_transform_events(events_df):
    # %%
    runfile("data_proc/transform.py")
    enricher = CodeDescEnricher()
    # %%
    data_dict = enricher.data_dicts["cameo_actor_type"]
    # %%
    enricher.enrich_ip(events_df, "a1_type1_code", "cameo_actor_type")
    # %%
    enricher.enrich_ip(events_df, "ev_code", "cameo_event")
    enricher.enrich_ip(events_df, "ev_base_code", "cameo_event")
    enricher.enrich_ip(events_df, "ev_root_code", "cameo_event")
    enricher.enrich_ip(events_df, "quad_class", "quad_classes")
    # %%
