"""Functions to produce labels of news pieces via LLMs"""
import json
import os
import re
import time
from datetime import datetime
from hashlib import sha256
from pprint import pformat
from re import Match
from typing import Optional

import anthropic
import pandas as pd
from anthropic import Anthropic, BaseModel
from dataset import Database, Table  # type: ignore # noqa: PGH003
from dataset.util import ResultIter  # type: ignore # noqa: PGH003
from groq import Groq

from data_proc.common import UTC, gdelt_base_data_path
from data_proc.news.scraping import TBL_SCRAPE_RESULTS, get_scraping_db
from data_proc.utils import batches_from_iter, try_int_or
from shared import logging, runpyfile

ANTHR_DEFAULT_MODEL = "claude-3-haiku-20240307"
GROQ_DEFAULT_MODEL = "llama-3.1-8b-instant"

L = logging.getLogger("nwslbl")

TBL_LABELS = "llm_labels"

EXTRACT_EVENTS_TMPL = """
    Please indicate whether the following piece news indicates any of the following situations/events:
      political_instability/civil_protests/economic_turmoil/common_crimes (such as robery)/accident/killing/kidnapping/military_actions/natural_disasters

    news piece follows:
    ```
    {news_text}
    ```

    Reply with only two lines:
    1. including the topics above  that are mentioned by the news piece, separated by commas.
    If none are mentioned, reply just 'None'
    2. Single number: the number of people affected/involved or victims of the action.
""" # noqa: E501
# %%


def _interactive_testing() -> None:
    # %%
    runpyfile("data_proc/news_extraction/news_labeling.py")
    model = GROQ_DEFAULT_MODEL
    max_tokens = 1024
    prompt_tmpl = EXTRACT_EVENTS_TMPL
    # %%
    a_str = EXTRACT_EVENTS_TMPL.format(news_text="")
    # %%
    client = _get_groq_client()
    # %%
    news_text = """
    BUENOS AIRES, May 10 (Xinhua) -- At least 30 people were seriously injured in a train accident in the Argentine capital of Buenos Aires on Friday, authorities said.

    Alberto Crescenti, director of the capital's Emergency Medical Assistance Service, said that 90 people were treated and 30 of them were referred to health centers under the qualification "code red," which means in serious condition, local media reported.

    Two of the people who were taken to the medical center had "traumatic head injuries and were taken by helicopter to Santojanni Hospital" in the capital, Crescenti said.

    The city's head of government, Jorge Macri, described the disaster as a "serious accident," and said the evacuation operation worked very well.

    An investigation was initiated to determine the cause of the accident, local railway authorities said.
    """ # noqa: E501
    # %%
    # %%
    print(a_str, client, model, max_tokens, prompt_tmpl, news_text)
    # %%


class LabelingResult(BaseModel):
    """Result returned by LLM"""

    detected_situations: list[str] | None = None
    affected_people_count: int | None = None
    raw_completion: str | None = None
    error_msg: str | None


# %%
class LabelingDbRecord(BaseModel):
    """Db Record of labelling result"""

    label_hash: str
    url_hash: str
    model: str
    prompt_tmpl_hash: str
    labeling_params: str
    label: LabelingResult
    success: bool
# %%

def run_labeling(prompt_tmpl: str, model: str) -> None:
    """Run incremental labelling using a given template and a model.

    Note: incremental means, we query to find out which texts already
    have a lebelling for this prompt_tmpl and model, and only run on
    those that don't yet
    """
    # %%
    labeling_params = {"max_text_len": 4000,
                       "max_tokens": 256}
    max_text_len = labeling_params["max_text_len"]
    prompt_tmpl_hash = _gen_prompt_tmpl_hash(prompt_tmpl)

    db = get_scraping_db()
    prompts_tbl = _get_prompt_tmpls_tbl(db)
    prompts_tbl.upsert({"prompt_tmpl_hash": prompt_tmpl_hash,
                        "prompt_tmpl": prompt_tmpl},
                       keys="prompt_tmpl_hash")
    db.commit()
    # %%
    pending_texts_iter = _get_pending_texts(db, prompt_tmpl_hash, model,
                                            limit=2000)
    client = _get_groq_client()

    tbl: Table = db[TBL_LABELS]
    size_est = tbl.count()

    for i, batch in enumerate(batches_from_iter(pending_texts_iter, batch_size=10)):
        L.info(f"Starting batch: {i} len(batch) = {len(batch)}")

        out_records = []
        for record in batch:
            news_text = record['scraped_text'][:max_text_len]
            result = label_news_groq(client,
                                  news_text=news_text,
                                  model=model,
                                  max_tokens=labeling_params['max_tokens'],
                                  prompt_tmpl=prompt_tmpl)

            success=(result.detected_situations is not None
                     and result.affected_people_count is not None)

            label_hash = _gen_hash(32, prompt_tmpl_hash, news_text, model,
                                   str(labeling_params))

            labeling_record = LabelingDbRecord(
                label_hash=label_hash,
                url_hash=record['url_hash'],
                label=result,
                model=model,
                labeling_params=json.dumps(labeling_params),
                prompt_tmpl_hash=prompt_tmpl_hash,
                success=success,
            )
            out_record = labeling_record.dict()
            out_record['label'] = labeling_record.label.to_json()

            out_records.append(out_record)

            time.sleep(2.0)

        tbl.upsert_many(out_records, keys=["label_hash"])
        size_est += len(out_records)
        db.commit()
        L.info(f"out_records has {len(out_records)}, size_est={size_est}, tbl_size={tbl.count()}")
    # %%

def _get_pending_texts(db: Database,
                       prompt_tmpl_hash: str, model: str,
                       limit: int) -> ResultIter:
    stmt = f"""with labels_pre as (
                select *
                from {TBL_LABELS}
                where prompt_tmpl_hash ='{prompt_tmpl_hash}'
                  and model = '{model}'
            )
            select r.url_hash, r.scraped_text
            from {TBL_SCRAPE_RESULTS} as r
            left join labels_pre as l
                on r.url_hash = l.url_hash
            where l.url_hash is null and r.scraped_text is not null
            order by r.url_hash
            limit {limit}
    """ # noqa: S608

    return db.query(stmt)
    # %%

def _get_prompt_tmpls_tbl(db: Database) -> Table:
    return db.create_table("prompt_tmpls",
                           primary_id="prompt_tmpl_hash",
                           primary_type=db.types.string)

def _get_labels_tbl(db: Database) -> Table:
    # %%
    run_column_creation = TBL_LABELS not in db

    labels_tbl: Table = db.create_table(TBL_LABELS,
                                        primary_id="label_hash",
                                        primary_type=db.types.string)

    if run_column_creation:
        L.info("Adding tables to table because it didn't exist")
        labels_tbl.create_column("url_hash", type=db.types.string)
        labels_tbl.create_column("prompt_tmpl_hash", type=db.types.string)
        labels_tbl.create_column("model", type=db.types.string)
        labels_tbl.create_column("labeling_params", type=db.types.string)
        labels_tbl.create_column("label", type=db.types.string)
        labels_tbl.create_column("success", type=db.types.boolean)
    # %%

    return labels_tbl


def _gen_hash(h_len: int, *args: str) -> str:
    concat = "".join(args)
    return sha256(concat.encode("utf8")).hexdigest()[:h_len]

def _gen_prompt_tmpl_hash(prompt_tmpl: str) -> str:
    pt = remove_indentation(prompt_tmpl.strip())
    return sha256(pt.encode("utf8")).hexdigest()[:12]


def _get_anthr_client() -> Anthropic:
    return anthropic.Anthropic()

def _get_groq_client() -> Groq:
    return Groq(api_key=os.environ["GROQ_API_KEY"])

#
# def label_news_anthropic(client: Anthropic, news_text: str,
#                          model: str = ANTHR_DEFAULT_MODEL,
#                          max_tokens: int = 1024,
#                          prompt_tmpl: str = EXTRACT_EVENTS_TMPL) -> list[str]:
#     """Extract key info form an anthropic model"""
#     compl_resp: Message = client.messages.create(
#         model=model,
#         max_tokens=max_tokens,
#         temperature=0.,
#         messages=[{
#             "role": "user",
#             "content": remove_indentation(prompt_tmpl.format(news_text=news_text)),
#         }],
#     )
#
#     return compl_resp.content[0].split("\n")


def label_news_groq(client: Groq, *, news_text: str,
                    model: str = GROQ_DEFAULT_MODEL,
                    max_tokens: int = 1024,
                    prompt_tmpl: str = EXTRACT_EVENTS_TMPL) -> LabelingResult:
    """Extract key info form an anthropic model"""
    # %%
    try:
        resp = client.chat.completions.create(
            model=model,
            max_tokens=max_tokens,
            temperature=0.,
            messages=[{
                "role": "user",
                "content": remove_indentation(prompt_tmpl.format(news_text=news_text)),
            }],
        )
    except Exception as exc:  # noqa: BLE001
        return LabelingResult(error_msg=f"API failure: {exc!r}")

    completion: str | None = resp.choices[0].message.content
    # %%
    if completion is None:
        L.error("Failed getting completion from grok: %s", pformat(completion))
        return LabelingResult(error_msg="completion is None")
    # %%
    lines = completion.split("\n")
    error_msg = None

    if len(lines) >= 1:
        line0 = lines[0].lower().strip()
        line0 = line0[2:].strip() if line0.startswith('2.') else line0

        if line0 == "none":
            detected_situations = []
        else:
            detected_situations = [piece for piece in lines[0][2:].strip().split(',')
                                   if piece != '']
    else:
        detected_situations = None
        error_msg = f'Not enough lines or invalid format for line 1: {completion!r}'
        L.error(error_msg)

    people_cnt: int | None = None
    if len(lines) >= 2:  # noqa: PLR2004
        line1 = lines[1].strip()
        line1 = line1[2:].strip() if line1.startswith('2.') else line1
        people_cnt = try_int_or(line1.replace(",", ""), default=None)

        if people_cnt is None:
            error_msg = f'Could not get an int from line 2: {line1!r}'
            L.error(error_msg)
    else:
        error_msg = f'Not enough lines or invalid format for line 2: {completion!r}'
        L.error(error_msg)

    # %%
    return LabelingResult(
        detected_situations=detected_situations,
        affected_people_count=people_cnt,
        raw_completion=completion,
        error_msg=error_msg,
    )

def remove_indentation(a_str: str) -> str:
    """Remove indentation from a multiline string."""
    lines = a_str.split("\n")

    # infer indent length from first indented line
    indents: list[Optional[Match]] = [ re.search(r'^ +', line) for line in lines ]
    first_ident_len = next(iter(len(mch.group(0))
                                for mch in indents if mch is not None), 0)

    remove_indent = ' ' * first_ident_len
    return "\n".join( line[first_ident_len:] if line.startswith(remove_indent) else line
                      for line in lines)
    # %%


def _dump_labels_to_parquet() -> None:
    # %%
    now_str = datetime.now(tz=UTC).isoformat()[:-13].replace(":", "")
    db = get_scraping_db()
    labels_tbl = db[TBL_LABELS]
    labels_df = pd.DataFrame(labels_tbl.all())
    labels_df.to_parquet(gdelt_base_data_path() / f"llm_labels.{now_str}.parquet")
    db.close()
    # %%
