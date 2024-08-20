create or replace view gdelt.events_agg_country_quad_class as (
   select
      -- action_country,
      ev_date,
      act_country_code as action_country_code,
      action_country,
      quad_class,
      quad_class_desc,
      count(*) as cnt,
      sum(gstein_scale) / count(*) as avg_gstein,
      sum(gstein_scale * num_mentions) / sum(num_mentions)
         as avg_gstein_weighted_mentions,
      sum(gstein_scale * ee.num_sources) / sum(num_sources)
         as avg_gstein_weighted_sources,
      sum(gstein_scale * ee.num_articles) / sum(num_articles)
         as avg_gstein_weighted_articles
   from gdelt.events_enriched as ee
   group by ev_date, act_country_code, action_country, quad_class, quad_class_desc
)
;

create or replace view gdelt.events_agg_country_ev_root as (
   select
      -- action_country,
      ev_date,
      act_country_code as action_country_code,
      action_country,
      ev_root_code,
      ev_root_desc,
      count(*) as cnt,
      sum(gstein_scale) / count(*) as avg_gstein,
      sum(gstein_scale * ee.num_mentions) / sum(num_mentions)
         as avg_gstein_weighted_mentions,
      sum(gstein_scale * ee.num_sources) / sum(num_sources)
         as avg_gstein_weighted_sources,
      sum(gstein_scale * ee.num_articles) / sum(num_articles)
         as avg_gstein_weighted_articles
   from gdelt.events_enriched as ee
   group by ev_date, act_country_code, action_country, ev_root_code, ev_root_desc
)
;

create or replace view gdelt.events_agg_country_ev_base as (
   select
      -- action_country,
      ev_date,
      act_country_code as action_country_code,
      action_country,
      ev_base_code,
      ev_base_desc,
      count(*) as cnt,
      sum(gstein_scale) / count(*) as avg_gstein,
      sum(gstein_scale * ee.num_mentions) / sum(num_mentions)
         as avg_gstein_weighted_mentions,
      sum(gstein_scale * ee.num_sources) / sum(num_sources)
         as avg_gstein_weighted_sources,
      sum(gstein_scale * ee.num_articles) / sum(num_articles)
         as avg_gstein_weighted_articles
   from gdelt.events_enriched as ee
   group by ev_date, act_country_code, action_country, ev_root_code, ev_root_desc
)
;

-- select * from gdelt.events_agg_country_quad_class;
