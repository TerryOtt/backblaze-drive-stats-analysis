# backblaze-drive-stats-analysis

**Updated 2025-10-26**

## Creating Quarterly AFR CSV

### System Requirements

1. a Python 3.x intepreter
1. Python `pip` to install this script's Python dependencies
1. 64+ GB of memory (process will be killed by the OS due to memory exhaustion with less)
1. Network access

I've tested on multiple architectures (both x86-64 amd ARM64) because, well, why not?

### Backblaze B2 Bucket Credentials

To get the access key/secret access key for read-only access to the B2 bucket, visit 
the [Backblaze source data site](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data)
and search for the string "**Application Key**." 

Both values are public because they only provide read-only access, but GitHub doesn't love seeing access keys committed to
repositories, so it's left as an exercise for the reader.

### Installation Steps

```
$ sudo apt-get update
$ sudo apt-get -y install python3-venv
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip3 install -r requirements.txt
$ python3 afr_viz_csv_from_iceberg.py   \
    drives_of_interest_regexes.json     \
    [s3_access_key]                     \
    [s3_secret_access_key]              \
    quarterly_afr_2025q3.csv

Opening Polars datasource...
        Current Backblaze Drive Stats Iceberg schema file: 00248-af7b2e6d-6381-4dcd-a949-a6f8f74ad51e.metadata.json
                Schema URI: s3://drivestats-iceberg/drivestats/metadata/00248-af7b2e6d-6381-4dcd-a949-a6f8f74ad51e.metadata.json

ETL pipeline stage 1 / 6: Retrieve candidate SMART drive model names...
        Retrieved 3 regexes for SMART drive model names from "drives_of_interest_regexes.json"
        Retrieving unique candidate SMART drive model names from Polars...
                Retrieved 38 candidate SMART drive model names in 25.6 seconds

ETL pipeline stage 2 / 6: Create mapping table for SMART model name -> normalized model name...
        38 SMART drive model names -> 34 normalized drive model names

ETL pipeline stage 3 / 6: Retrieve deploy counts for drive models...
        Retrieving drive deployment counts from Polars...
                Retrieved drive deploy counts for 19 candidate drive models in 22.2 seconds

        INFO: 15 candidate drive models were filtered out due to drive counts < 2,000
                (modify with --min-drives)

ETL pipeline stage 4 / 6: Retrieve AFR calculation input data...
        Retrieving daily drive health data from Polars for 19 drive models...
        Reading incremental results batches, max rows per batch = 16,384 (modify with --max-batch)
        Retrieved 32,815 rows of drive health data from Polars
        Retrieved drive health data in 19.2 seconds

ETL pipeline stage 5 / 6: Perform AFR calculations...
        Quarterly AFR calculations completed

ETL pipeline stage 6 / 6: Writing AFR data to visualization CSV...
        Creating visualization CSV file "quarterly_afr_2025q3.csv"
        Max quarters of AFR data for any drive model: 33

ETL pipeline total processing time: 67.8 seconds

	
$ ls -laF quarterly_afr_2025q3.csv 
-rw-rw-r-- 1 ubuntu ubuntu 3326 Oct 20 20:39 quarterly_afr_2025q3.csv 

$ wc -l quarterly_afr_2025q3.csv 
34 quarterly_afr_2025q3.csv 
```

## Runtime Data
 
This section demonstrates the effects of various amounts of CPU and RAM
resources when running this script.

I _happened_ to test on AWS EC2 instances as it was convenient for me.

Tests were performed in AWS's `us-west-2` region, as `us-west-2` is the lowest latency 
region to the Backblaze data which offers `*8a` instances (currently only `m8a` 
as of October 2025.

* **m8a.4xlarge** (16 C / 16 T CPU, 64 GB memory):
* **m8a.8xlarge** (32 C / 32 T CPU, 128 GB memory):
* **m8a.12xlarge** (48 C / 48 T CPU, 192 GB memory):
* **m8a.16xlarge** (64 C / 64 T CPU, 256 GB memory):
* **m8a.24xlarge** (96 C / 96 T CPU, 384 GB memory):
* **m8a.48xlarge** (192 C / 192 T CPU, 768 GB memory): 
