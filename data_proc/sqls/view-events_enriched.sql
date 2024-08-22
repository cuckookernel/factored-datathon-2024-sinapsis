create or replace view gdelt.events_enriched as (
  with pre1 as (
      select
          split(action_geo_full_name, ',') as g_name_parts,
          size(split(action_geo_full_name, ',')) as g_name_len,
          ev.*
      from gdelt.last_1y_events as ev
  ),
  pre2 as (
      select
          trim(get(g_name_parts, g_name_len - 1)) as country,
          trim(get(g_name_parts, g_name_len - 2)) as state,
          trim(get(g_name_parts, g_name_len - 3)) as city,
          pre1.*
      from pre1
  )
  select
    qc.ev_desc as quad_class_desc,
    cec_r.ev_desc as ev_root_desc,
    cec_b.ev_desc as ev_base_desc,
    cec_f.ev_desc as ev_desc,
    ev.*,
    fca1.country_name as a1_country,
    fca2.country_name as a2_country,
    fcact.country_name as action_geo_country,
    case when ev.action_geo_country_code in ('US', 'CH', 'BR', 'IS', 'IN', 'UK', 'RS') then
        concat(country, "/", state)
    else
        concat(country)
    end as geo_zone,
    cat1.a_type_desc as a1_type1_desc,
    cat2.a_type_desc as a2_type1_desc
  from pre2 as ev
  left join gdelt.gdelt_quad_classes as qc
    on ev.quad_class = qc.quad_class
  left join gdelt.cameo_ev_codes as cec_r
    on ev.ev_root_code = cec_r.ev_code
  left join gdelt.cameo_ev_codes as cec_b
    on ev.ev_base_code = cec_b.ev_code
  left join gdelt.cameo_ev_codes as cec_f
    on ev.ev_code = cec_f.ev_code
  --- country codes for a1, a2, action
  left join gdelt.fips_country_codes as fca1
    on ev.a1_geo_country_code = fca1.fips_country_code
  left join gdelt.fips_country_codes as fca2
    on ev.a2_geo_country_code = fca2.fips_country_code
  left join gdelt.fips_country_codes as fcact
    on ev.action_geo_country_code = fcact.fips_country_code
  left join gdelt.cameo_actor_type as cat1
    on ev.a1_type1_code = cat1.a_type_code
  left join gdelt.cameo_actor_type as cat2
    on ev.a2_type1_code = cat1.a_type_code
)
;
