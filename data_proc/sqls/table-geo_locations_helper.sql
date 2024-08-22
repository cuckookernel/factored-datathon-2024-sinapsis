create or replace table gdelt.geo_locations_helper as
with pre as (
   select
       country, state, city,
       median(action_geo_lat) as lat,
       median(action_geo_lon) as lon,
       count(1) as ev_count
   from gdelt.events_enriched
   where action_geo_lat is not null and action_geo_lon is not null
   GROUP BY ROLLUP(country, state, city)
)
select
    (if(country is null, 0, 1)
     + if(state is null, 0, 1)
      + if(city is null, 0, 1)) as geo_granularity,
    pre.*
from pre
where pre.country is not null
;
