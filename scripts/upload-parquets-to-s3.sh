#!/usr/bin/env bash
# Remember to do export AWS_PROFILE='...'  before ...
has_param() {
    local term="$1"
    shift
    for arg; do
        if [[ $arg == "$term" ]]; then
            return 0
        fi
    done
    return 1
}

set -xe

local_directory="$1"
shift
s3_directory="$1"
shift

if [[ -z "$s3_directory" ]]; then
  echo "Need two arguments <local_directory> <s3_directory>"

  echo "
    Example

    FULL REFRESH MODEL
      i.e. clearing S3 destination completely before uploading any local files!:


    scripts/upload-parquets-to-s3.sh $DATA_DIR/GDELT/last_1y_events     last_1y_events
    scripts/upload-parquets-to-s3.sh $DATA_DIR/GDELT/last_1y_gkg        last_1y_gkg
    scripts/upload-parquets-to-s3.sh $DATA_DIR/GDELT/last_1y_gkgcounts  last_1y_gkgcounts

    INCREMENTAL MODEL:
      i.e. only adding files to S3 (use with caution as it might cause data duplication!

    scripts/upload-parquets-to-s3.sh $DATA_DIR/GDELT/last_1y_events     last_1y_events  --incremental
    scripts/upload-parquets-to-s3.sh $DATA_DIR/GDELT/last_1y_gkg        last_1y_gkg     --incremental
    scripts/upload-parquets-to-s3.sh $DATA_DIR/GDELT/last_1y_gkgcounts  last_1y_gkgcounts  --incremental
  "
  exit 1
fi


if has_param '--incremental'; then
  echo "INFO: incremental WILL NOT CLEARN S3 DIRECTORY first"
else
  echo "Clearing s3 first..."
  aws s3 rm --recursive $S3_PREFIX/$s3_directory
fi

aws s3 cp --recursive $local_directory/raw_parquet/  $S3_PREFIX/$s3_directory

aws s3 ls $S3_PREFIX/$s3_directory/
