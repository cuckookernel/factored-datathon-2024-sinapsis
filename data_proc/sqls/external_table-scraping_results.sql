create external table gdelt.scraping_results
(
    part_date date comment 'Corresponds to partition date on events or gkg_counts tables',
    source_url string comment 'the scraped url',
    scraped_text string comment 'the plain scraped text (after extracting from html)',
    status_code int comment 'the http status code obtained when scraping',
    url_hash string comment 'uniquely identifies url',
    scraped_len int comment 'number of bytes of html response',
    scraped_text_len int comment 'number of characters of plain text'
)
using PARQUET
comment 'Extracted texts from news urls '
location 's3://databricks-workspace-stack-41100-bucket/unity-catalog/2578366185261862/scraping_results/'
;
