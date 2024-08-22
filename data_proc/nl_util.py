"""Natural language utils"""
from pandas import Series, DataFrame

from data_proc.utils import is_not_na


def compute_svp_descriptions(events_df: DataFrame) -> DataFrame:
    """Compute full text descriptions with the format (Actor 1) (action) (Actor2)"""
    ret = events_df[['date_added', 'ev_id', 'action_geo_country_code',
                     'action_geo_country', 'ev_date']].copy()
    ret['svp_desc'] = events_df.apply(svp_description_for_event, axis=1)
    return ret


def svp_description_for_event(event: Series) -> str:
    """Create a full description of the form below

    (Actor 1) (action) (Actor2) this happend in (geo_full_name), was reported in (source_url)
    """
    if is_not_na(event['a1_name']):
        actor1_pieces = [
                f"Actor 1: {event['a1_name']}" if is_not_na(event['a1_name']) else "",
                f"(type: {event['a1_type1_desc']} )" if is_not_na(event['a1_type1_desc']) else "",
        ]
        actor1_part = " ".join(actor1_pieces).strip()
    else:
        actor1_part = ""

    action_part = f"performed action {event['ev_root_desc']}, more specifically {event['ev_desc']}"

    if is_not_na(event['a2_name']):
        actor2_pieces = [
                f"on Actor 2: {event['a2_name']}" if is_not_na(event['a2_name']) else "",
                f"(type: {event['a2_type1_desc']})" if is_not_na(event['a2_type1_desc']) else "",
        ]
        actor2_part = " ".join(actor2_pieces).strip()
    else:
        actor2_part = ""

    geo_full_part = f" This took place in: {event['action_geo_full_name']}"
    source_part = f" and was reported in: {event['source_url']}"

    return f"{actor1_part} {action_part} {actor2_part} {geo_full_part} {source_part}"
