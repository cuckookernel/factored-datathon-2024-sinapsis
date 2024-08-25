create or replace table gdelt.heat_indicator_by_date_location_v1
as 
with pre as (
  select 
      indicator_date, country_code, geo_zone, country, ev_id,
      log(1 +  num_mentions) as log_num_mentions,
      log(1 +  num_mentions) * heat_indicator as weighted_heat
  from gdelt.heat_indicator_by_event
)
select 
    indicator_date, 
    country_code,
    country,
    geo_zone,
    count(ev_id) 
        as frequency,
    sum(weighted_heat) / sum(log_num_mentions) 
        as heat_indicator
from pre 
group by 1, 2, 3, 4
;

drop table if exists gdelt.heat_indicator_by_date_location;
alter table gdelt.heat_indicator_by_date_location_v1 rename to gdelt.heat_indicator_by_date_location;