# backblaze-drive-stats-analysis

**Updated 2025-10-26**

## Creating Quarterly AFR CSV

### System Requirements

1. a Python 3.x intepreter
1. Python `pip` to install this script's Python dependencies
1. 8+ GB of memory (process will be killed by the OS due to memory exhaustion with less)
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

Tests were performed in AWS's `us-west-1` region, as it has the lowest latency to 
the Backblaze data. Per-minute `us-west-1` pricing (USD) is from 2025-10-28. 

* **c7i.24xlarge** (48C/96T CPU, 192 GB memory): 4.1 minutes
  * 0.37 USD (4.1 minutes @ 0.08904 USD / minute)
* **c7i.16xlarge** (32C/64T CPU, 128 GB memory): 4.3 minutes
  * 0.26 USD (4.3 minutes @ 0.05936 USD / minute)
* **c7i.12xlarge** (24C/48T CPU, 96 GB memory): 4.4 minutes
  * 0.20 USD (4.4 minutes @ 0.04452 USD / minute)
* **m7i.8xlarge** (16C/32T CPU, 128 GB memory): 5.0 minutes
  * 0.16 USD (5.0 minutes @ 0.03136 USD / minute)
* **c7i.8xlarge** (16C/32T CPU, 64 GB memory): 5.3 minutes
  * 0.16 USD (5.3 minutes @ 0.02968 USD / minute)
* **r7i.4xlarge** (8C/16T CPU, 128 GB memory): 6.0 minutes
  * 0.12 USD (6.0 minutes @ 0.01960 USD / minute)
* **m7i.4xlarge** (8C/16T CPU, 64 GB memory): 6.0 minutes
  * 0.10 USD (6.0 minutes @ 0.01568 USD / minute)
* **c7i.4xlarge** (8C/16T CPU, 32 GB memory): 6.7 minutes
  * 0.10 USD (6.7 minutes @ 0.01484 USD / minute)
* **r7i.2xlarge** (4C / 8T CPU, 64 GB memory): 7.7 minutes
  * 0.08 USD (7.7 minutes @ 0.00980 USD / minute)
* **m7i.2xlarge** (4C / 8T CPU, 32 GB memory): 7.8 minutes
  * 0.07 USD (7.8 minutes @ 0.00784 USD / minute)
* **c7i.2xlarge** (4C / 8T CPU, 16 GB memory): 9.7 minutes
  * 0.07 USD (9.7 minutes @ 0.00742 USD / minute)
* **r7i.xlarge** (2C / 4T CPU, 32 GB memory): 11.2 minutes
  * 0.06 USD (11.2 mintes @ 0.00490 USD / minute)
* **m7i.xlarge** (2C / 4T CPU, 16 GB memory): 11.6 minutes
  * 0.05 (11.6 minutes @ 0.00392 USD / minute)
* **c7i.xlarge** (2C / 4T CPU, 8 GB memory): 11.8 minutes
  * 0.05 USD (11.8 minutes @ 0.00371 USD / minute)
* **r7i.large** (1C / 2T CPU, 16 GB memory): 18.3 minutes
  * 0.05 USD (18.3 minutes @ 0.00245 USD / minute)
* **m7i.large** (1C / 2T CPU, 8 GB memory): 26.2 minutes
  * 0.06 USD (26.2 minutes @ 0.00196 USD / minute)
