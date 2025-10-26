# backblaze-drive-stats-analysis

**Updated 2025-10-26**

## Creating Quarterly AFR CSV

**NOTE**: 32 GB of memory is _required_, otherwise the process will be killed by the OS due to memory exhaustion.

To get the access key/secret access key for read-only access to the B2 bucket, visit 
the [Backblaze source data site](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data)
and search for the string "**Application Key**." 

Both values are public because they only provide read-only access, but GitHub doesn't love seeing access keys committed to
repositories, so it's left as an exercise for the reader.

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

I _happened_ to test on AWS EC2 instances as it was convenient, but to be clear, 
the script runs fine on anything with the following three things:
* a Python 3.x intepreter
* 32+ GB of memory
* Network access

I've even tested on multiple architectures (both x86-64 amd ARM64) because, well, 
why not?

Pricing is `us-west-1` prices (USD) as of 2025-10-26

* **c7i.24xlarge** (48C/96T CPU, 192 GB memory): 2.3 minutes
  * 0.20 USD
* **c7i.16xlarge** (32C/64T CPU, 128 GB memory): 2.5 minutes
  * 0.15 USD
* **c7i.12xlarge** (24C/48T CPU, 96 GB memory): 2.6 minutes
  * 0.12 USD
* **m7i.8xlarge** (16C/32T CPU, 128 GB memory): 3.1 minutes
  * 0.09 USD
* **c7i.8xlarge** (16C/32T CPU, 64 GB memory): 3.1 minutes
  * 0.09 USD
* **m7i.4xlarge** (8C/16T CPU, 64 GB memory): 4.4 minutes
  * 0.07 USD
* **c7i.4xlarge** (8C/16T CPU, 32 GB memory): 5.3 minutes
  * 0.08 USD
* **m7i.2xlarge** (4C/8T CPU, 32 GB memory): 8.6 minutes
  * 0.06 USD
* **r7i.xlarge** (2C/4T CPU, 32 GB memory): 14.0 minutes
  * 0.07 USD

_Note_: runtime tests were performed in AWS's `us-west-1` region, as it has the lowest latency to 
the Backblaze data.
