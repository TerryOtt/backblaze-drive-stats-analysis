# backblaze-drive-stats-analysis

**Updated 2025-11-02**

## Creating Quarterly AFR CSV

### System Requirements

1. a Python 3.x intepreter
1. Python `pip` to install this script's Python dependencies
1. 64+ GB of memory (process will be killed by the OS due to memory exhaustion with less)
1. Network access
1. Permission to write to S3 destination (optional)

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
```

### Running The Script

```
$ python3 quarterly_afr_xlsx_from_iceberg.py 	        \
    drives_of_interest_regexes.json     		        \
    [s3_access_key]                     		        \
    [s3_secret_access_key]              		        \
    s3://bucket_name/bucket_path/quarterly_afr.xlsx

Opening Polars datasource...
        Current Backblaze Drive Stats Iceberg schema file: 00248-af7b2e6d-6381-4dcd-a949-a6f8f74ad51e.metadata.json
                Schema URI: s3://drivestats-iceberg/drivestats/metadata/00248-af7b2e6d-6381-4dcd-a949-a6f8f74ad51e.metadata.json

ETL pipeline stage 1 of 5: Retrieve candidate SMART drive model names...
        Retrieved 3 regexes for SMART drive model names from "drives_of_interest_regexes.json"
        Retrieving unique candidate SMART drive model names from Polars...
                Retrieved 38 candidate SMART drive model names in 10.9 seconds

ETL pipeline stage 2 of 5: Create mapping table for SMART model name -> normalized model name...
        38 SMART drive model names -> 34 normalized drive model names

ETL pipeline stage 3 of 5: Perform AFR calculations...
        Operation time: 16.8 seconds

ETL pipeline stage 4 of 5: Enrich viz data with quarterly drive deploys/removals...
        Data enriched with quarterly drive deploys/removals in 7.0 seconds

ETL pipeline stage 5 of 5: Generating XLSX for visualizing Backblaze drive stats quarterly data...
        Created output XLSX: s3://bucket_name/bucket_path/quarterly_afr_2025q3.xlsx

ETL pipeline total processing time: 36.2 seconds
```

## Runtime Data
 
This section demonstrates the effects of various amounts of CPU and RAM
resources when running this script.

I _happened_ to test on AWS EC2 instances as it was convenient for me.

Tests were performed in AWS's `us-west-2` region, as `us-west-2` is the lowest latency 
region to the Backblaze data which offers `*8a` instances (currently only `m8a` 
as of October 2025).

* **m8a.4xlarge** (16 C / 16 T CPU, 64 GB memory): 76 seconds
* **m8a.8xlarge** (32 C / 32 T CPU, 128 GB memory): 50 seconds
* **m8a.12xlarge** (48 C / 48 T CPU, 192 GB memory): 39 seconds
* **m8a.16xlarge** (64 C / 64 T CPU, 256 GB memory): 39 seconds
