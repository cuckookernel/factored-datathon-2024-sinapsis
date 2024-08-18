"""Functions to produce labels of news pieces via LLMs"""
import os
import re
from dataclasses import dataclass
from pprint import pformat, pprint
from re import Match
from typing import Optional

import anthropic
from anthropic import Anthropic
from groq import Groq

from data_proc.utils import try_int_or
from shared import logging, runpyfile

ANTHR_DEFAULT_MODEL = "claude-3-haiku-20240307"
GROQ_DEFAULT_MODEL = "llama-3.1-8b-instant"

L = logging.getLogger("nwslbl")
# %%


def _interactive_testing() -> None:
    # %%
    runpyfile("data_proc/news_extraction/news_labeling.py")
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
    model = GROQ_DEFAULT_MODEL
    max_tokens = 1024
    prompt_tmpl = EXTRACT_EVENTS_TMPL
    # %%
    print(a_str, client, model, max_tokens, prompt_tmpl, news_text)
    # %%



def _get_anthr_client() -> Anthropic:
    return anthropic.Anthropic()

# %%
def _get_groq_client() -> Groq:
    return Groq(
        # This is the default and can be omitted
        api_key=os.environ["GROQ_API_KEY"],
    )

# %%
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

@dataclass
class LabelResult:
    """Result returned by LLM"""

    detected_situations: Optional[list[str]]
    affected_people_count: Optional[int]

# %%
def label_news_anthropic(client: Anthropic, news_text: str,
                         model: str = ANTHR_DEFAULT_MODEL,
                         max_tokens: int = 1024,
                         prompt_tmpl: str = EXTRACT_EVENTS_TMPL) -> list[str]:
    """Extract key info form an anthropic model"""
    compl_resp = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        temperature=0.,
        messages=[{
            "role": "user",
            "content": remove_indentation(prompt_tmpl.format(news_text=news_text)),
        }],
    )

    return compl_resp.content[0].split("\n")


def label_news_grop(client: Groq, news_text: str,
                    model: str = GROQ_DEFAULT_MODEL,
                    max_tokens: int = 1024,
                    prompt_tmpl: str = EXTRACT_EVENTS_TMPL) -> Optional[LabelResult]:
    """Extract key info form an anthropic model"""
    # %%
    resp = client.chat.completions.create(
        model=model,
        max_tokens=max_tokens,
        temperature=0.,
        messages=[{
            "role": "user",
            "content": remove_indentation(prompt_tmpl.format(news_text=news_text)),
        }],
    )

    pprint(resp.dict())
    completion: str | None = resp.choices[0].message.content
    # %%
    if completion is None:
        L.error("Failed getting completion from grok: %s", pformat(completion))
        return None
    # %%
    lines = completion.split("\n")

    if len(lines) >= 1 and lines[0].startswith('1.'):
        detected_situations = [piece for piece in lines[0][2:].strip().split(',') if piece != '']
    else:
        detected_situations = None
        L.error(f'Invalid not enough lines or invalid format for line 1: {completion!r}')

    people_cnt: int | None = None
    if len(lines) >= 2 and lines[1].startswith('2.'):  # noqa: PLR2004
        people_cnt = try_int_or( lines[1][2:].strip(), default=None)

        if people_cnt is None:
            L.error(f'Could not get an int from line 2: {lines[1]!r}')
    else:
        L.error(f'Invalid not enough lines or invalid format for line 2: {completion!r}')
    # %%
    return LabelResult(
        detected_situations=detected_situations,
        affected_people_count=people_cnt,
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
