This directory contains CLI scripts to download data, convert to parquet etc.

Before running scripts make sure to run:

```shell
source .venv/bin/activate  # activate venv
source ~/profile.env  # ensure necessary env vars are defined in the shell
```

After that you can run scripts without arguments to just get the help


Under Linux, Mac, run script directly (it should already have execute permissions and a line such as `#/usr/bin/env`):

```shell
scripts/any_script.py
```

Under Windows you might need to precede this with `python3` (not sure...)

```shell
python3 scripts/any_script.py
```


## Example Usages:


```shell
scripts/download_raw_csvs.py events 2024-05-12 2024-08-12 $DATA_DIR/GDELT/v1_3months -v1 
[22:34:27] INFO     rich logger initialized                                                                                           __init__.py:16
           INFO     url_list has: 92 items                                                                                   download_raw_csvs.py:43
                    first: http://data.gdeltproject.org/events/20240512.export.CSV.zip                                                              
                    last:  http://data.gdeltproject.org/events/20240811.export.CSV.zip                                                              
                                                                                                                                                    
           INFO     Output dir is: /home/teo/data/GDELT/v1_3months/raw_data                                                  download_raw_csvs.py:48
[22:34:58] INFO     Progress: 10.9 % | Files downloaded: 10 | not downloaded (were already there): 0 | last file:                    download.py:305
                    20240521.export.CSV.zip | total size: 71.7 MiB | elapsed time: 30.67 secs | download rate: 2.34 MiB/s                           
...
           INFO     Done writing files to: /home/teo/data/GDELT/v1_3months/raw_data  
```


```shell
# Random sample of 0.1% of the GKG data fir kast five years:
scripts/download_raw_csvs.py -v1 gkg 2019-08-12 2024-08-12 -f 0.01 \$DATA_DIR/GDELT/last_5yrs_gkg
[23:53:04] INFO     rich logger initialized                                                                                    __init__.py:15
[23:53:05] INFO     url_list has: 1827 items                                                                          download_raw_csvs.py:46
                    first: http://data.gdeltproject.org/gkg/20190812.gkg.csv.zip                                                             
                    last:  http://data.gdeltproject.org/gkg/20240811.gkg.csv.zip                                                             
                                                                                                                                             
           INFO     Output dir is: $DATA_DIR/GDELT/last_5yrs_gkg/raw_data                                             download_raw_csvs.py:51



```

```shell
scripts/generate_parquets.py -v2 events $DATA_DIR/GDELT/last_5yrs/

# Log:
[10:18:43] INFO     rich logger initialized                                                                                                                                               __init__.py:16
[10:18:44] WARNING  Zero files found in fpath: /home/teo/data/GDELT/last_5yrs/raw_data/20190817.export.CSV.sampled_0.1.zip, skipping                                                         load.py:206
[10:18:45] INFO     Saving parquet chunk: /home/teo/data/GDELT/last_5yrs/raw_parquet/20190812-20190829.export.sampled_0.1.parquet                                                            load.py:141
           INFO     SaveParquetStats(raw_file_cnt=17, raw_bytes_read=13985108, row_cnt=265398, parquet_file_cnt=0, parquet_bytes_written=0, parquet_rows_written=0) compression:0            load.py:106
[10:18:48] INFO     Saving parquet chunk: /home/teo/data/GDELT/last_5yrs/raw_parquet/20190830-20190916.export.sampled_0.1.parquet                                                            load.py:141
           INFO     SaveParquetStats(raw_file_cnt=35, raw_bytes_read=27828245, row_cnt=527689, parquet_file_cnt=1, parquet_bytes_written=34716404, parquet_rows_written=265398)              load.py:106
                    compression:65.79                                                                                                                                                                   
[10:18:51] INFO     Saving parquet chunk: /home/teo/data/GDELT/last_5yrs/raw_parquet/20190917-20191002.export.sampled_0.1.parquet                                                            load.py:141
           INFO     SaveParquetStats(raw_file_cnt=51, raw_bytes_read=41338920, row_cnt=786523, parquet_file_cnt=2, parquet_bytes_written=68957454, parquet_rows_written=527689)              load.py:106
                    compression:87.67                                                                                                                                                                   
[10:18:54] INFO     Saving parquet chunk: /home/teo/data/GDELT/last_5yrs/raw_parquet/20191003-20191018.export.sampled_0.1.parquet                                                            load.py:141
           INFO     SaveParquetStats(raw_file_cnt=67, raw_bytes_read=54627737, row_cnt=1040784, parquet_file_cnt=3, parquet_bytes_written=102874069, parquet_rows_written=786523)            load.py:106
                    compression:98.84                                                                                                                                                                   
[10:18:57] INFO     Saving parquet chunk: /home/teo/data/GDELT/last_5yrs/raw_parquet/20191019-20191105.export.sampled_0.1.parquet                                                            load.py:141
           INFO     SaveParquetStats(raw_file_cnt=85, raw_bytes_read=68490171, row_cnt=1302459, parquet_file_cnt=4, parquet_bytes_written=136218278, parquet_rows_written=1040784)           load.py:106
                    compression:104.6                      ```


```shell
# Command for Mentions data:
scripts/generate_parquets.py mentions /home/teo/data/GDELT/raw_data/ -v 2

# Output
fname: 20240809110000.mentions.CSV.zip - 3935 records
fname: 20240809201500.mentions.CSV.zip - 4373 records
...
fname: 20240809061500.mentions.CSV.zip - 2410 records
fname: 20240809054500.mentions.CSV.zip - 2649 records
save_parquet: created file /home/teo/data/GDELT/raw_parquet/2408090000-2408092345.mentions.parquet, data shape: (346294, 15)
```
