from pyspark.sql.types import DateType, FloatType, IntegerType, StringType, StructField, StructType


class DeltaTableHelper:
    class BronzeTables:
        gkg_counts_table = "gdelt.bronze_gkgcounts"
        gkg_counts_schema = StructType(
            [
                StructField("pub_date", IntegerType(), True),
                StructField("num_articles", IntegerType(), True),
                StructField("count_type", StringType(), True),
                StructField("reported_count", FloatType(), True),
                StructField("object_type", StringType(), True),
                StructField("geo_type", IntegerType(), True),
                StructField("geo_full_name", StringType(), True),
                StructField("geo_country_code", StringType(), True),
                StructField("geo_adm1_code", StringType(), True),
                StructField("geo_lat", FloatType(), True),
                StructField("geo_lon", FloatType(), True),
                StructField("geo_feat_id", StringType(), True),
                StructField("event_ids", StringType(), True),
                StructField("sources", StringType(), True),
                StructField("source_urls", StringType(), True),
            ]
        )

        gkg_table = "gdelt.bronze_gkg"
        gkg_schema = StructType(
            [
                StructField("pub_date", IntegerType(), True),
                StructField("num_articles", IntegerType(), True),
                StructField("counts", StringType(), True),
                StructField("themes", StringType(), True),
                StructField("locations", StringType(), True),
                StructField("persons", StringType(), True),
                StructField("organizations", StringType(), True),
                StructField("tone_vec", StringType(), True),
                StructField("event_ids", StringType(), True),
                StructField("sources", StringType(), True),
                StructField("source_urls", StringType(), True),
            ]
        )

        event_table = "gdelt.bronze_events"
        event_schema = StructType(
            [
                StructField("ev_id", IntegerType(), nullable=True),
                StructField("ev_date", IntegerType(), nullable=True),
                StructField("year_month", IntegerType(), nullable=True),
                StructField("year", IntegerType(), nullable=True),
                StructField("date_fraction", FloatType(), nullable=True),
                StructField("a1_code", StringType(), nullable=True),
                StructField("a1_name", StringType(), nullable=True),
                StructField("a1_country_code", StringType(), nullable=True),
                StructField("a1_group_code", StringType(), nullable=True),
                StructField("a1_ethnic_code", StringType(), nullable=True),
                StructField("a1_rel1_code", StringType(), nullable=True),
                StructField("a1_rel2_code", StringType(), nullable=True),
                StructField("a1_type1_code", StringType(), nullable=True),
                StructField("a1_type2_code", StringType(), nullable=True),
                StructField("a1_type3_code", StringType(), nullable=True),
                StructField("a2_code", StringType(), nullable=True),
                StructField("a2_name", StringType(), nullable=True),
                StructField("a2_country_code", StringType(), nullable=True),
                StructField("a2_group_code", StringType(), nullable=True),
                StructField("a2_ethnic_code", StringType(), nullable=True),
                StructField("a2_rel1_code", StringType(), nullable=True),
                StructField("a2_rel2_code", StringType(), nullable=True),
                StructField("a2_type1_code", StringType(), nullable=True),
                StructField("a2_type2_code", StringType(), nullable=True),
                StructField("a2_type3_code", StringType(), nullable=True),
                StructField("is_root_event", IntegerType(), nullable=True),
                StructField("ev_code", StringType(), nullable=True),
                StructField("ev_base_code", StringType(), nullable=True),
                StructField("ev_root_code", StringType(), nullable=True),
                StructField("quad_class", IntegerType(), nullable=True),
                StructField("gstein_scale", FloatType(), nullable=True),
                StructField("num_mentions", IntegerType(), nullable=True),
                StructField("num_sources", IntegerType(), nullable=True),
                StructField("num_articles", IntegerType(), nullable=True),
                StructField("avg_tone", FloatType(), nullable=True),
                StructField("a1_geo_type", IntegerType(), nullable=True),
                StructField("a1_geo_full_name", StringType(), nullable=True),
                StructField("a1_geo_country_code", StringType(), nullable=True),
                StructField("a1_geo_adm1_code", StringType(), nullable=True),
                StructField("a1_geo_lat", FloatType(), nullable=True),
                StructField("a1_geo_lon", FloatType(), nullable=True),
                StructField("a1_geo_feat_id", StringType(), nullable=True),
                StructField("a2_geo_type", IntegerType(), nullable=True),
                StructField("a2_geo_full_name", StringType(), nullable=True),
                StructField("a2_geo_country_code", StringType(), nullable=True),
                StructField("a2_adm1_code", StringType(), nullable=True),
                StructField("a2_geo_lat", FloatType(), nullable=True),
                StructField("a2_geo_lon", FloatType(), nullable=True),
                StructField("a2_geo_feat_id", StringType(), nullable=True),
                StructField("action_geo_type", IntegerType(), nullable=True),
                StructField("action_geo_full_name", StringType(), nullable=True),
                StructField("action_geo_country_code", StringType(), nullable=True),
                StructField("action_geo_adm1_code", StringType(), nullable=True),
                StructField("action_geo_lat", FloatType(), nullable=True),
                StructField("action_geo_lon", FloatType(), nullable=True),
                StructField("action_geo_feat_id", StringType(), nullable=True),
                StructField("date_added", IntegerType(), nullable=True),
                StructField("source_url", StringType(), nullable=True),
            ]
        )

        schema_map = {"gkg": gkg_schema, "events": event_schema, "gkg_counts": gkg_counts_schema}
        table_map = {"gkg": gkg_table, "events": event_table, "gkg_counts": gkg_counts_table}

    class SilverTables:
        class Events:
            table_name = "gdelt.silver_events"
            partition = "ev_date"
            schema = StructType(
                [
                    StructField("ev_id", IntegerType(), nullable=True),
                    StructField("ev_date", DateType(), nullable=True),
                    StructField("a1_name", StringType(), nullable=True),
                    StructField("a1_country_code", StringType(), nullable=True),
                    StructField("a1_type1_code", StringType(), nullable=True),
                    StructField("a1_type2_code", StringType(), nullable=True),
                    StructField("a2_name", StringType(), nullable=True),
                    StructField("a2_country_code", StringType(), nullable=True),
                    StructField("a2_type1_code", StringType(), nullable=True),
                    StructField("a2_type2_code", StringType(), nullable=True),
                    StructField("is_root_event", IntegerType(), nullable=True),
                    StructField("ev_code", StringType(), nullable=True),
                    StructField("ev_base_code", StringType(), nullable=True),
                    StructField("ev_root_code", StringType(), nullable=True),
                    StructField("quad_class", IntegerType(), nullable=True),
                    StructField("gstein_scale", FloatType(), nullable=True),
                    StructField("num_mentions", IntegerType(), nullable=True),
                    StructField("num_sources", IntegerType(), nullable=True),
                    StructField("num_articles", IntegerType(), nullable=True),
                    StructField("avg_tone", FloatType(), nullable=True),
                    StructField("a1_geo_type", IntegerType(), nullable=True),
                    StructField("a1_geo_full_name", StringType(), nullable=True),
                    StructField("a1_geo_adm1_code", StringType(), nullable=True),
                    StructField("a1_geo_lat", FloatType(), nullable=True),
                    StructField("a1_geo_lon", FloatType(), nullable=True),
                    StructField("a1_geo_feat_id", StringType(), nullable=True),
                    StructField("a2_geo_type", IntegerType(), nullable=True),
                    StructField("a2_geo_full_name", StringType(), nullable=True),
                    StructField("a2_adm1_code", StringType(), nullable=True),
                    StructField("a2_geo_lat", FloatType(), nullable=True),
                    StructField("a2_geo_lon", FloatType(), nullable=True),
                    StructField("a2_geo_feat_id", StringType(), nullable=True),
                    StructField("action_geo_type", IntegerType(), nullable=True),
                    StructField("action_geo_full_name", StringType(), nullable=True),
                    StructField("action_geo_country_code", StringType(), nullable=True),
                    StructField("action_geo_adm1_code", StringType(), nullable=True),
                    StructField("action_geo_lat", FloatType(), nullable=True),
                    StructField("action_geo_lon", FloatType(), nullable=True),
                    StructField("action_geo_feat_id", StringType(), nullable=True),
                    StructField("date_added", DateType(), nullable=True),
                    StructField("source_url", StringType(), nullable=True),
                ]
            )
