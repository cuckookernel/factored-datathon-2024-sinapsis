create or replace table gdelt.geo_zones_helper as
select
    action_geo_country_code as country_code,
    max(action_geo_country) as country,
    geo_zone,
    median(action_geo_lat) as lat,
    median(action_geo_lon) as lon,
    count(1) as ev_count
from gdelt.events_enriched
where
        action_geo_lat is not null
    and action_geo_lon is not null
    and country is not null
    and geo_zone is not null
GROUP BY country_code, geo_zone
;
