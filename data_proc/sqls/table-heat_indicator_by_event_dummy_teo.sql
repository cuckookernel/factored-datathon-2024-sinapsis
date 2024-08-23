create or replace table gdelt.heat_indicator_by_event_dummy_teo
partitioned by (heat_date)
as (select
  ev_date as heat_date,
  ev_id,  -- PK
  action_geo_country_code as country_code,
  geo_zone,
  quad_class_desc,
  ev_root_desc,
  ev_desc,
  log(1 + num_mentions) * (-gstein_scale) as ev_heat,
  action_geo_lat as lat,
  action_geo_lon as lon,
  source_url,
  quad_class,
  ev_root_code,
  ev_base_code,
  ev_code
from
  gdelt.events_enriched
where
   is_root_event = 1
)
;
