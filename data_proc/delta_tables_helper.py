"""Bronze and Silver tables declarations"""
from typing import ClassVar

from pyspark.sql.functions import udf
from pyspark.sql.types import (
    ArrayType,
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


class DeltaTableHelper:
    class BronzeTables:
        """Declares all bronze tables"""

        gkg_counts_table = "gdelt.bronze_gkgcounts"
        gkg_counts_schema = StructType(
            [
                StructField("pub_date", IntegerType(), True),
                StructField("num_articles", IntegerType(), True),
                StructField("count_type", StringType(), True),
                StructField("reported_count", FloatType(), True),
                StructField("object_type", StringType(), True),
                StructField("geo_id", IntegerType(), True),
                StructField("geo_full_name", StringType(), True),
                StructField("geo_country_code", StringType(), True),
                StructField("geo_adm1_code", StringType(), True),
                StructField("geo_lat", FloatType(), True),
                StructField("geo_lon", FloatType(), True),
                StructField("geo_feat_id", StringType(), True),
                StructField("event_ids", StringType(), True),
                StructField("sources", StringType(), True),
                StructField("source_urls", StringType(), True),
            ],
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
            ],
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
                StructField("a1_geo_id", IntegerType(), nullable=True),
                StructField("a1_geo_full_name", StringType(), nullable=True),
                StructField("a1_geo_country_code", StringType(), nullable=True),
                StructField("a1_geo_adm1_code", StringType(), nullable=True),
                StructField("a1_geo_lat", FloatType(), nullable=True),
                StructField("a1_geo_lon", FloatType(), nullable=True),
                StructField("a1_geo_feat_id", StringType(), nullable=True),
                StructField("a2_geo_id", IntegerType(), nullable=True),
                StructField("a2_geo_full_name", StringType(), nullable=True),
                StructField("a2_geo_country_code", StringType(), nullable=True),
                StructField("a2_adm1_code", StringType(), nullable=True),
                StructField("a2_geo_lat", FloatType(), nullable=True),
                StructField("a2_geo_lon", FloatType(), nullable=True),
                StructField("a2_geo_feat_id", StringType(), nullable=True),
                StructField("action_geo_id", IntegerType(), nullable=True),
                StructField("action_geo_full_name", StringType(), nullable=True),
                StructField("action_geo_country_code", StringType(), nullable=True),
                StructField("action_geo_adm1_code", StringType(), nullable=True),
                StructField("action_geo_lat", FloatType(), nullable=True),
                StructField("action_geo_lon", FloatType(), nullable=True),
                StructField("action_geo_feat_id", StringType(), nullable=True),
                StructField("date_added", IntegerType(), nullable=True),
                StructField("source_url", StringType(), nullable=True),
            ],
        )

        schema_map: ClassVar = {"gkg": gkg_schema, "events": event_schema, "gkg_counts": gkg_counts_schema}
        table_map: ClassVar = {"gkg": gkg_table, "events": event_table, "gkg_counts": gkg_counts_table}

    class SilverTables:
        class Events:
            table_name = "gdelt.silver_events"
            partition = "date_added"
            schema = StructType(
                [
                    StructField("ev_id", IntegerType(), nullable=True),
                    StructField("ev_date", DateType(), nullable=True),
                    StructField("a1_name", StringType(), nullable=True),
                    StructField("a1_country_code", StringType(), nullable=True),
                    StructField("a1_country_name", StringType(), nullable=True),
                    StructField("a1_type1_code", StringType(), nullable=True),
                    StructField("a1_type1_desc", StringType(), nullable=True),
                    StructField("a1_type2_code", StringType(), nullable=True),
                    StructField("a1_type2_desc", StringType(), nullable=True),
                    StructField("a1_geo_id", IntegerType(), nullable=True),
                    StructField("a1_geo_type", StringType(), nullable=True),
                    StructField("a1_geo_country_code", StringType(), nullable=True),
                    StructField("a1_geo_country", StringType(), nullable=True),
                    StructField("a1_geo_state", StringType(), nullable=True),
                    StructField("a1_geo_location", StringType(), nullable=True),
                    StructField("a1_geo_lat", FloatType(), nullable=True),
                    StructField("a1_geo_lon", FloatType(), nullable=True),
                    StructField("a2_name", StringType(), nullable=True),
                    StructField("a2_geo_country_code", StringType(), nullable=True),
                    StructField("a2_country_code", StringType(), nullable=True),
                    StructField("a2_country_name", StringType(), nullable=True),
                    StructField("a2_type1_code", StringType(), nullable=True),
                    StructField("a2_type1_desc", StringType(), nullable=True),
                    StructField("a2_type2_code", StringType(), nullable=True),
                    StructField("a2_type2_desc", StringType(), nullable=True),
                    StructField("a2_geo_id", IntegerType(), nullable=True),
                    StructField("a2_geo_type", StringType(), nullable=True),
                    StructField("a2_geo_country", StringType(), nullable=True),
                    StructField("a2_geo_state", StringType(), nullable=True),
                    StructField("a2_geo_location", StringType(), nullable=True),
                    StructField("a2_geo_lat", FloatType(), nullable=True),
                    StructField("a2_geo_lon", FloatType(), nullable=True),
                    StructField("action_geo_country_code", StringType(), nullable=True),
                    StructField("action_geo_country", StringType(), nullable=True),
                    StructField("action_geo_state", StringType(), nullable=True),
                    StructField("action_geo_location", StringType(), nullable=True),
                    StructField("action_geo_id", IntegerType(), nullable=True),
                    StructField("action_geo_type", StringType(), nullable=True),
                    StructField("action_geo_lat", FloatType(), nullable=True),
                    StructField("action_geo_lon", FloatType(), nullable=True),
                    StructField("is_root_event", IntegerType(), nullable=True),
                    StructField("ev_code", StringType(), nullable=True),
                    StructField("ev_desc", StringType(), nullable=True),
                    StructField("ev_base_code", StringType(), nullable=True),
                    StructField("ev_base_desc", StringType(), nullable=True),
                    StructField("ev_root_code", StringType(), nullable=True),
                    StructField("ev_root_desc", StringType(), nullable=True),
                    StructField("quad_class", IntegerType(), nullable=True),
                    StructField("quad_class_desc", StringType(), nullable=True),
                    StructField("gstein_scale", FloatType(), nullable=True),
                    StructField("num_mentions", IntegerType(), nullable=True),
                    StructField("num_sources", IntegerType(), nullable=True),
                    StructField("num_articles", IntegerType(), nullable=True),
                    StructField("avg_tone", FloatType(), nullable=True),
                    StructField("date_added", DateType(), nullable=True),
                    StructField("source_url", StringType(), nullable=True),
                ],
            )

        class GKG:
            table_name = "gdelt.silver_gkg"
            partition = "pub_date"
            schema = StructType(
                [
                    StructField("pub_date", IntegerType(), True),
                    StructField("num_articles", IntegerType(), True),
                    StructField(
                        "counts",
                        ArrayType(
                            StructType(
                                [
                                    StructField("Type", StringType(), True),
                                    StructField("Count", IntegerType(), True),
                                    StructField("ObjectType", StringType(), True),
                                    StructField("LocationType", StringType(), True),
                                    StructField("LocationName", StringType(), True),
                                    StructField("CountryCode", StringType(), True),
                                    StructField("ADM1Code", StringType(), True),
                                    StructField("Latitude", FloatType(), True),
                                    StructField("Longitude", FloatType(), True),
                                    StructField("LocationFeatureID", StringType(), True),
                                ],
                            ),
                        ),
                        True,
                    ),
                    StructField("themes", ArrayType(StringType(), True), True),
                    StructField(
                        "locations",
                        ArrayType(
                            StructType(
                                [
                                    StructField("LocationType", StringType(), True),
                                    StructField("LocationName", StringType(), True),
                                    StructField("CountryCode", StringType(), True),
                                    StructField("ADM1Code", StringType(), True),
                                    StructField("Latitude", FloatType(), True),
                                    StructField("Longitude", FloatType(), True),
                                    StructField("LocationFeatureID", StringType(), True),
                                ],
                            ),
                        ),
                        True,
                    ),
                    StructField("persons", ArrayType(StringType(), True), True),
                    StructField("organizations", ArrayType(StringType(), True), True),
                    StructField(
                        "tone_vec",
                        StructType(
                            [
                                StructField("Tone", FloatType(), True),
                                StructField("PositiveScore", FloatType(), True),
                                StructField("NegativeScore", FloatType(), True),
                                StructField("Polarity", FloatType(), True),
                                StructField("ActRefDensity", FloatType(), True),
                                StructField("SelfGroupRefDensity", FloatType(), True),
                            ],
                        ),
                        True,
                    ),
                    StructField("event_ids", ArrayType(StringType(), True), True),
                    StructField("sources", ArrayType(StringType(), True), True),
                    StructField("source_urls", ArrayType(StringType(), True), True),
                ],
            )

            @staticmethod
            def parse_gkg_count_entry(count_entry: str | None) -> list[dict] | None:
                loc_type_map = {
                    "1": "COUNTRY",
                    "2": "USSTATE",
                    "3": "USCITY",
                    "4": "WORLDCITY",
                    "5": "WORLDSTATE",
                }
                parsed_entries = []
                if count_entry:
                    entries = count_entry.split(";")
                    for entry in entries:
                        if entry:
                            parts = entry.split("#")
                            parsed_entry = {
                                "Type": parts[0],
                                "Count": int(parts[1])
                                if len(parts) > 1 and parts[1].isdigit()
                                else None,
                                "ObjectType": parts[2] if len(parts) > 2 else None,
                                "LocationType": loc_type_map.get(parts[3], parts[3])
                                if len(parts) > 3 and parts[3]
                                else None,
                                "LocationName": parts[4] if len(parts) > 4 else None,
                                "CountryCode": parts[5] if len(parts) > 5 else None,
                                "ADM1Code": parts[6] if len(parts) > 6 else None,
                                "Latitude": float(parts[7])
                                if len(parts) > 7 and parts[7]
                                else None,
                                "Longitude": float(parts[8])
                                if len(parts) > 8 and parts[8]
                                else None,
                                "LocationFeatureID": parts[9] if len(parts) > 9 else None,
                            }
                            parsed_entries.append(parsed_entry)

                # Return None if no entries were found
                return parsed_entries if len(parsed_entries) > 0 else None

            gkg_counts_schema = ArrayType(
                StructType(
                    [
                        StructField("Type", StringType(), True),
                        StructField("Count", IntegerType(), True),
                        StructField("ObjectType", StringType(), True),
                        StructField("LocationType", StringType(), True),
                        StructField("LocationName", StringType(), True),
                        StructField("CountryCode", StringType(), True),
                        StructField("ADM1Code", StringType(), True),
                        StructField("Latitude", FloatType(), True),
                        StructField("Longitude", FloatType(), True),
                        StructField("LocationFeatureID", StringType(), True),
                    ],
                ),
            )

            parse_gkg_count_entry_udf = udf(parse_gkg_count_entry, gkg_counts_schema)

            @staticmethod
            def parse_gkg_location_entry(count_entry: str) -> list[dict] | None:
                loc_type_map = {
                    "1": "COUNTRY",
                    "2": "USSTATE",
                    "3": "USCITY",
                    "4": "WORLDCITY",
                    "5": "WORLDSTATE",
                }
                parsed_entries = []
                if count_entry is not None and count_entry != "":
                    entries = count_entry.split(";")
                    for entry in entries:
                        if entry:
                            parts = entry.split("#")
                            parsed_entry = {
                                "LocationType": loc_type_map.get(parts[0], parts[0])
                                if len(parts) > 0 and parts[0]
                                else None,
                                "LocationName": parts[1] if len(parts) > 1 else None,
                                "CountryCode": parts[2] if len(parts) > 2 else None,
                                "ADM1Code": parts[3] if len(parts) > 3 else None,
                                "Latitude": float(parts[4])
                                if len(parts) > 4 and parts[4]
                                else None,
                                "Longitude": float(parts[5])
                                if len(parts) > 5 and parts[5]
                                else None,
                                "LocationFeatureID": parts[6] if len(parts) > 6 else None,
                            }
                            parsed_entries.append(parsed_entry)

                # Return None if no entries were found
                return parsed_entries if len(parsed_entries) > 0 else None

            gkg_location_schema = ArrayType(
                StructType(
                    [
                        StructField("LocationType", StringType(), True),
                        StructField("LocationName", StringType(), True),
                        StructField("CountryCode", StringType(), True),
                        StructField("ADM1Code", StringType(), True),
                        StructField("Latitude", FloatType(), True),
                        StructField("Longitude", FloatType(), True),
                        StructField("LocationFeatureID", StringType(), True),
                    ],
                ),
            )

            parse_gkg_location_entry_udf = udf(parse_gkg_location_entry, gkg_location_schema)

            @staticmethod
            def parse_tone(tone_entry) -> dict:
                # Split the comma-separated string and convert to floats
                if tone_entry:
                    values = tone_entry.split(",")
                    return {
                        "Tone": float(values[0]) if len(values) > 0 else None,
                        "PositiveScore": float(values[1]) if len(values) > 1 else None,
                        "NegativeScore": float(values[2]) if len(values) > 2 else None,
                        "Polarity": float(values[3]) if len(values) > 3 else None,
                        "ActRefDensity": float(values[4]) if len(values) > 4 else None,
                        "SelfGroupRefDensity": float(values[5]) if len(values) > 5 else None,
                    }
                return {}

            tone_schema = StructType(
                [
                    StructField("Tone", FloatType(), True),
                    StructField("PositiveScore", FloatType(), True),
                    StructField("NegativeScore", FloatType(), True),
                    StructField("Polarity", FloatType(), True),
                    StructField("ActRefDensity", FloatType(), True),
                    StructField("SelfGroupRefDensity", FloatType(), True),
                ],
            )
            parse_tone_udf = udf(parse_tone, tone_schema)

        class GKGCounts:
            table_name = "gdelt.silver_gkgcounts"
            partition = "pub_date"
            schema = StructType(
                [
                    StructField("pub_date", DateType(), True),
                    StructField("num_articles", IntegerType(), True),
                    StructField("count_type", StringType(), True),
                    StructField("reported_count", FloatType(), True),
                    StructField("object_type", StringType(), True),
                    StructField("geo_id", IntegerType(), True),
                    StructField("geo_type", StringType(), True),
                    StructField("geo_country_code", StringType(), True),
                    StructField("geo_country", StringType(), True),
                    StructField("geo_state", StringType(), True),
                    StructField("geo_location", StringType(), True),
                    StructField("geo_lat", FloatType(), True),
                    StructField("geo_lon", FloatType(), True),
                    StructField("event_ids", ArrayType(IntegerType()), True),
                    StructField("sources", ArrayType(StringType()), True),
                    StructField("source_urls", ArrayType(StringType()), True),
                ],
            )
