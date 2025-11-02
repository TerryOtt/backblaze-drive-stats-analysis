#!/usr/bin/env bash

# Check that we got a year-quarter argument
if [ "$#" -eq 0 ]; then
    echo
    echo "Usage: $0 <year_quarter, e.g. \"2025q3\">"
    echo "  year_quarter value of \"2025q3\" will create \"s3://backblaze-drive-stats.sixbuckssolutions.com/afr_data/2025q3_quarterly_afr.xlsx\""
    echo
    exit 1
fi

OUTPUT_XLSX_S3_URI="s3://backblaze-drive-stats.sixbuckssolutions.com/afr_data/${1}_quarterly_afr.xlsx"
PYTHON3="python3"

source .venv/bin/activate

$PYTHON3 quarterly_afr_xlsx_from_iceberg.py \
drives_of_interest_regexes.json \
0045f0571db506a0000000017 \
K004Fs/bgmTk5dgo6GAVm2Waj3Ka+TE \
"${OUTPUT_XLSX_S3_URI}"
