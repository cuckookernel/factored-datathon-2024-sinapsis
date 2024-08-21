import joblib  # type: ignore
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from databricks.sql.client import Connection
from pandas import DataFrame, Series
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import PCA  # type: ignore

import modeling.embeddings as membs
from data_proc.common import gdelt_base_data_path
from modeling.embeddings import ByCacheEmbedder
from shared import assert_type, logging, runpyfile
from shared.databricks_conn import get_sql_conn

L = logging.getLogger("cr_embdrs")
# %%


def main():
    # %%
    db_conn = get_sql_conn()
    # %%
    all_countries_seq = _get_countries(db_conn)
    _, embedder = create_country_name_embedder(all_countries_seq)
    joblib.dump(embedder, gdelt_base_data_path() / "country_embedder.pkl")
    # %%
    ev_descs = _get_event_descriptions(db_conn)
    pca_decomp, embedder = create_ev_desc_embedder(ev_descs, final_dim=40)
    joblib.dump(embedder, gdelt_base_data_path() / "event_full_desc_embedder.pkl")
    # %%



def _interactive_testing(db_conn: Connection):
    # %%
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
    geo_names_df = _get_geo_full_names(db_conn, limit=9000) # roughly 91% frequency coverage

    # %%
    transformer_name = "all-MiniLM-L6-v2"
    model = SentenceTransformer(transformer_name)
    text_values = list("This event happened in: "
                               + geo_names_df['action_geo_full_name'])
    embeds: np.ndarray = assert_type(model.encode(text_values), np.ndarray)
    # %%
    pca = PCA(n_components=100) # ~
    reduced_embeds = pca.fit_transform(embeds)
    # %%
    pca.explained_variance_ratio_.cumsum()
    # %%
    from importlib import reload
    reload(membs)

    gfn_embedder = membs.ByCacheAndBackupEmbedder(
                    raw_values=list(geo_names_df['action_geo_full_name']),
                    reduced_embeddings=reduced_embeds,
                    pca=pca,
                    bkup_transformer_name=transformer_name,
                )

    gfn_embedder.persist(gdelt_base_data_path() / "geo_full_name_embedder.pkl")
    # %%
    geo_names_df2 = _get_geo_full_names(db_conn, limit=50000)
    # %%
    gfn_embedder.transform(list(geo_names_df2['action_geo_full_name']))
    # %%


def _get_countries(db_conn: Connection) -> Series:
    all_countries = pd.read_sql("""
                select action_country as country, 
                    log(1 + count(1)) as log_1p_cnt
                from gdelt.events_enriched
                group by action_country
        """, con=db_conn)

    return all_countries['country']


def create_country_name_embedder(all_countries_seq: Series,
                                 final_dim: int = 30, # empirically this achives 70% of explained variance
                                 ) -> tuple[PCA, ByCacheEmbedder]:
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
    # Load https://huggingface.co/sentence-transformers/all-mpnet-base-v2
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
                select action_geo_full_name, 
                    count(1) as cnt,
                    log(1 + count(1)) as log_1p_cnt
                from gdelt.events_enriched
                group by action_geo_full_name
                order by cnt desc
                limit {limit}
        """, con=db_conn)

    geo_full_names_df['fraction'] = geo_full_names_df['cnt'] / total_cnt
    geo_full_names_df = geo_full_names_df.sort_values('fraction', ascending=False)
    geo_full_names_df = geo_full_names_df.reset_index(drop=True)
    geo_full_names_df['fraction_cumsum'] = (geo_full_names_df['fraction'] # type: ignore
                                            .values.cumsum())
    # geo_full_names_df['fraction_cum_sum'] = geo_full_names_df.cumsum()

    return geo_full_names_df
