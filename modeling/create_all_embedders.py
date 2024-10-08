"""Create embedders for geo_full_name, event descriptions etc..."""
import joblib  # type: ignore # noqa: PGH003
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from databricks.sql.client import Connection
from pandas import DataFrame, Series
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import PCA  # type: ignore # noqa: PGH003

import modeling.embedders as membs
from data_proc.common import gdelt_base_data_path
from modeling.embedders import ByCacheEmbedder
from shared import assert_type, logging, runpyfile
from shared.databricks_conn import get_sql_conn

L = logging.getLogger("cr_embdrs")
# %%


def _main() -> None:
    # %%
    db_conn = get_sql_conn()
    # %%
    all_countries_seq = _get_countries(db_conn)
    _, embedder = create_country_name_embedder(all_countries_seq)
    out_fpath = gdelt_base_data_path() / "country_embedder.pkl"
    joblib.dump(embedder, out_fpath)
    L.info(f"Saved country embedder to: {out_fpath}  ({out_fpath.lstat().st_size} bytes)")
    # %%
    ev_descs = _get_event_descriptions(db_conn)
    pca_decomp, embedder = create_ev_desc_embedder(ev_descs, final_dim=40)
    out_fpath = gdelt_base_data_path() / "event_full_desc_embedder.pkl"
    joblib.dump(embedder, out_fpath)
    L.info(f"Saved country embedder to: {out_fpath}  ({out_fpath.lstat().st_size} bytes)")
    # %%
    geo_names_df = _get_geo_full_names(db_conn, limit=9000) # roughly 91% frequency coverage
    # %%
    transformer_name = "all-MiniLM-L6-v2"
    gfn_embedder = make_geo_full_name_embedder(geo_names_df, transformer_name)
    gfn_embedder.persist(gdelt_base_data_path() / "geo_full_name_embedder.pkl")
    # %%



def _interactive_testing(db_conn: Connection) -> None:
    # %%
    from importlib import reload
    reload(membs)
    runpyfile("modeling/create_all_embedders.py")
    # %%
    all_countries_seq = _get_countries(db_conn)
    # %%
    pca_decomp, country_embedder = create_country_name_embedder(all_countries_seq, final_dim=30)

    fig = plt.figure()
    ax = fig.add_subplot()
    ax.plot(pca_decomp.explained_variance_ratio_.cumsum())
    fig.show()
    # country_embedder.transform(list(all_countries_seq))
    # %%
    ev_descs = _get_event_descriptions(db_conn)
    # %%
    pca_decomp, embedder = create_ev_desc_embedder(ev_descs, final_dim=40) # almost 90% variance...
    fig = plt.figure()
    ax = fig.add_subplot()
    ax.plot(pca_decomp.explained_variance_ratio_.cumsum())
    fig.show()
    # %%
    geo_names_df2 = _get_geo_full_names(db_conn, limit=50000)
    # %%
    fpath = gdelt_base_data_path() / "geo_full_name_embedder.pkl"
    gfn_embedder = membs.ByCacheAndBackupEmbedder.restore_from_file(fpath)
    # %%
    texts = "This event happened in: " + geo_names_df2['geo_full_name']
    _ = gfn_embedder.transform(texts)
    # %%


def make_geo_full_name_embedder(geo_names_df: DataFrame,
                                transformer_name: str) -> membs.ByCacheAndBackupEmbedder:
    """Make and embedder with caching from the names in geo_names_df['geo_full_name']"""
    model = SentenceTransformer(transformer_name)
    geo_full_names = geo_names_df['geo_full_name']
    text_values = list("This event happened in: " + geo_full_names)
    embeds: np.ndarray = assert_type(model.encode(text_values), np.ndarray)

    pca = PCA(n_components=100) # ~
    reduced_embeds = pca.fit_transform(embeds)
    L.info("Explained variance ratio cum sum=%s", pca.explained_variance_ratio_.cumsum())
    # %%
    return membs.ByCacheAndBackupEmbedder(
                       text_values=text_values,
                       reduced_embeddings=reduced_embeds,
                       pca=pca,
                       bkup_transformer_name=transformer_name,
           )
    # %%


def _get_countries(db_conn: Connection) -> Series:
    all_countries = pd.read_sql("""
                select action_geo_country as country,
                    log(1 + count(1)) as log_1p_cnt
                from gdelt.events_enriched
                group by action_geo_country
    """, con=db_conn)

    return all_countries['country']



def create_country_name_embedder(all_countries_seq: Series,
                                 final_dim: int = 30,
                                 ) -> tuple[PCA, ByCacheEmbedder]:
    """Create low dimensionality country name embedder that uses cache"""
    # empirically this achives 70% of explained variance
    # Load https://huggingface.co/sentence-transformers/all-mpnet-base-v2
    model = SentenceTransformer("all-MiniLM-L6-v2")
    embeds = model.encode( list("This event happened in the country of "
                                + all_countries_seq) )

    pca_decomp = PCA(n_components=final_dim)
    reduced_embeds = pca_decomp.fit_transform(embeds)

    country_embedder = ByCacheEmbedder(raw_values=list(all_countries_seq),
                                       embeddings=reduced_embeds)

    return pca_decomp, country_embedder



def _get_event_descriptions(db_conn: Connection) -> DataFrame:
    event_descs_df = pd.read_sql("""
                select quad_class_desc, ev_root_desc, ev_desc as ev_desc,
                    log(1 + count(1)) as log_1p_cnt
                from gdelt.events_enriched
                group by quad_class_desc, ev_root_desc, ev_desc
        """, con=db_conn)

    event_descs_df['full_desc'] = (event_descs_df['quad_class_desc'] + ' - '
                                   + event_descs_df['ev_root_desc'].fillna('') + ' - '
                                   + event_descs_df['ev_desc']).fillna('')

    return event_descs_df


def create_ev_desc_embedder(all_descs_df: DataFrame,
                            final_dim: int,
                           ) -> tuple[PCA, ByCacheEmbedder]:
    """Create evending for "full event descriptions"

    full event descriptions should have the format: "<quad_class_desc> - <ev_root_desc> - <ev_desc>"
    otherwise cache won't work and errors will be logged...
    """
    model = SentenceTransformer("all-MiniLM-L6-v2")
    full_descs = all_descs_df['full_desc']
    embeds = model.encode( list("Actor 1 did the following to Actor 2: "
                                + full_descs) )

    pca_decomp = PCA(n_components=final_dim)
    reduced_embeds = pca_decomp.fit_transform(embeds)
    country_embedder = ByCacheEmbedder(raw_values=list(full_descs),
                                       embeddings=reduced_embeds)

    return pca_decomp, country_embedder


def _get_geo_full_names(db_conn: Connection, limit: int) -> DataFrame:
    total_cnt = pd.read_sql("""
                            select
                                count(1) as cnt
                            from gdelt.events_enriched
                            """,
                            con=db_conn)["cnt"].iloc[0]

    geo_full_names_df = pd.read_sql(f"""
                select action_geo_full_name as geo_full_name,
                    count(1) as cnt,
                    log(1 + count(1)) as log_1p_cnt
                from gdelt.events_enriched
                group by action_geo_full_name
                order by cnt desc
                limit {limit}
        """, # noqa: S608 # only called by our own code, no exposed
    con=db_conn)

    geo_full_names_df['fraction'] = geo_full_names_df['cnt'] / total_cnt
    geo_full_names_df = geo_full_names_df.sort_values('fraction', ascending=False)
    geo_full_names_df = geo_full_names_df.reset_index(drop=True)
    # type: ignore # noqa: PGH003
    geo_full_names_df['fraction_cumsum'] = geo_full_names_df['fraction'].to_numpy().cumsum()

    return geo_full_names_df
