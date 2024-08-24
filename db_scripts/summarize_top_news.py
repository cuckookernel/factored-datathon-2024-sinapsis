# Databricks notebook source
# MAGIC %pip install groq anthropic dataset deflate python-dotenv

# COMMAND ----------

dbutils.library.restartPython()


# COMMAND ----------

"""Summarization of selected news via LLMs"""
import json
import logging
import os
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date
from typing import TypeAlias

import pyspark.sql as ps
from groq import Groq
from pyspark.sql.types import BooleanType, DateType, Row, StringType, StructField, StructType

from data_proc.news.labeling import GROQ_DEFAULT_MODEL, remove_indentation
from shared import assert_type

SUMMARIZE_LLM_TMPL = """
    Please summarize the following piece of news in no more than 50 words:
    ```
    {news_text}
    ```
"""
logging.getLogger().setLevel("WARN")
LLMReqParams: TypeAlias = dict[str, str | int | None]


# COMMAND ----------


def n_grok_api_keys() -> int:
    return len(os.environ["GROQ_API_KEYS"].split(";"))

@dataclass
class SummarizeResult:
    """Result of summarization"""

    ext_id: str
    summary: str | None
    model: str
    prompt: str
    error_msg: str | None
    success: bool
    llm_req_params: dict[str, None | int | str]
    part_date: date

def res_to_dict(res: SummarizeResult) -> dict:
    """Represent myself as dict"""
    # ret = asdict(res) DOESN'T WORK?!!!
    ret = {}
    ret['ext_id'] = res.ext_id
    ret['summary']  = res.summary
    ret['model'] = res.model
    ret['prompt'] = res.prompt
    ret['error_msg'] = res.error_msg
    ret['success'] = res.success
    ret['llm_req_params'] = json.dumps(res.llm_req_params)
    ret['part_date'] = res.part_date
    return ret

SummarizeResultSchema = StructType([
    StructField("ext_id", StringType(), nullable=False),
    StructField("summary", StringType(), nullable=True),
    StructField("model", StringType(), nullable=False),
    StructField("prompt", StringType(), nullable=False),
    StructField("success", BooleanType(), nullable=False),
    StructField("error_msg", StringType(), nullable=True),
    StructField("llm_req_params", StringType(), nullable=True),
    StructField("part_date", DateType(), nullable=False),
])

def make_partition_with_index_mapper(*,
                          id_col: str,
                          news_text_col: str,
                          part_date_col: str,
                          groq_model: str, prompt_tmpl: str,
                          llm_req_params: LLMReqParams,
                          ) -> Callable[[int, Iterable[Row]], Iterable[dict]]:
    """Make a function that can summarize the contents of a whole mandas dataframe"""

    def _row_iterator_mapper_mapper(part_idx: int, input_: Iterable[Row]) -> Iterable[dict]:
        for row in input_:
            summary_res = summarize_one_with_grok(
                ext_id=row[id_col],
                news_text=row[news_text_col],
                part_date=row[part_date_col],
                api_key_idx=part_idx,
                groq_model=groq_model,
                prompt_tmpl=prompt_tmpl,
                llm_req_params=llm_req_params)

            yield summary_res

    return _row_iterator_mapper_mapper

def summarize_one_with_grok(*,
                            ext_id: str,
                            news_text: str | None,
                            part_date: date,
                            api_key_idx: int,
                            groq_model: str,
                            prompt_tmpl: str,
                            llm_req_params: LLMReqParams) -> dict:
    """Summarize one piece of text via Grok"""
    api_key = os.environ['GROQ_API_KEYS'].split(";")[api_key_idx]
    client = Groq(api_key=api_key)

    truncation_len=llm_req_params["input_trunc_len"]
    news_text_trunc = news_text[:]
    prompt = remove_indentation(prompt_tmpl.format(news_text=news_text_trunc))
    result = SummarizeResult(ext_id=ext_id,
                             part_date=part_date,
                             model=groq_model, summary=None, prompt=prompt,
                             success=False,
                             error_msg=None, llm_req_params=llm_req_params)

    try:
        max_tokens = assert_type(llm_req_params.get("max_tokens", 1024), int)
        resp = client.chat.completions.create(
            model=groq_model,
            max_tokens=max_tokens,
            temperature=0.,
            messages=[{
                "role": "user",
                "content": prompt,
            }],
        )
        result.success = True
        result.summary = resp.choices[0].message.content
        time.sleep(3.0)
        return res_to_dict(result)

    except Exception as exc:  # noqa: BLE001
        result.error_msg = f"API failure: {exc!r}"
        return res_to_dict(result)


# COMMAND ----------

res =  SummarizeResult(ext_id='adasd', summary=None, model='a', prompt='b', error_msg=None, success=False, llm_req_params={}, part_date=date(2013, 1, 1))
res_to_dict(res)


# COMMAND ----------

part_date_start = date(2023, 8, 10)
part_date_end = date(2023, 8, 12)

# COMMAND ----------

sdf: ps.DataFrame = spark.sql(f"""select * 
        from gdelt.scraping_results
        where scraped_text is not Null 
        and part_date >= '{part_date_start}'
        and part_date <= '{part_date_end}'
    """)
sdf = sdf.repartition(numPartitions=n_grok_api_keys()).cache()

sdf.limit(30).display()

# COMMAND ----------

partition_row_iter_mapper = make_partition_with_index_mapper(
          id_col="url_hash",
          news_text_col="scraped_text",
          part_date_col="part_date",
          groq_model=GROQ_DEFAULT_MODEL,
          prompt_tmpl=SUMMARIZE_LLM_TMPL,
          llm_req_params={"max_tokens": 1024,
                          "input_trunc_len": 2000}
    )

summaries_rdd = (sdf
                 .rdd.mapPartitionsWithIndex(partition_row_iter_mapper)
                ).cache()


# COMMAND ----------

collected = summaries_rdd.collect()
collected[:100]

# COMMAND ----------

summaries_df = spark.createDataFrame(summaries_rdd, schema=SummarizeResultSchema)
summaries_df.limit(10).display()

# COMMAND ----------

(summaries_df
    .write.mode("overwrite")
    .option("replaceWhere", f"part_date >= '{part_date_start}' and part_date <= '{part_date_end}'")
    .partitionBy("part_date")
    .saveAsTable("gdelt.summary_results"))


# COMMAND ----------


