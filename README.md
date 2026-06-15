# backblaze-drive-stats-analysis

**Updated 2026-06-14**

## Creating Quarterly AFR CSV

### System Requirements

1. sudo access (to install a C compiler and a system-wide version of uv)
1. 64+ GB of memory (process will be killed by the OS due to memory exhaustion with less)
1. Network access
1. Permission to write to S3 destination (optional)

The scripts have been tested on both x86-64 amd ARM64 architectures. 

### Backblaze B2 Bucket Credentials

To get the access key/secret access key for read-only access to the B2 bucket, visit 
the [Backblaze source data site](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data)
and search for the string "**Application Key**." 

Both values are public because they only provide read-only access, but GitHub doesn't love seeing access keys committed to
repositories, so it's left as an exercise for the reader.

### Installation Steps

```bash
$ sudo apt-get update
$ sudo apt-get -y upgrade
$ sudo apt-get -y install build-essential
$ curl -LsSf https://astral.sh/uv/install.sh | sudo env UV_INSTALL_DIR=/usr/local/bin INSTALLER_NO_MODIFY_PATH=1 sh
$ ./install-latest-uv-python-env.sh
```

### Running The Script

```
$ uv run quarterly_afr_xlsx_from_iceberg.py 	        \
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
 
This section demonstrates the effects of various amounts of CPU, RAM, and network
bandwidth resources when running this script.

I _happened_ to test on AWS EC2 instances as it was convenient for me.

Tests were performed in AWS's `us-west-1` region due to it being the lowest latency 
region to the Backblaze B2 bucket where the Iceberg table is stored.

* **8 vCPU Graviton4 ARM64 Instances**
  * **r8g.2xlarge** (8 C / 8 T CPU, 64 GB memory): 161 seconds
* **16 vCPU Instances**
  * **m8g.4xlarge** (16 C / 16 T CPU, 64 GB memory, up to 15 Gbps network): 120 seconds
  * **r8g.4xlarge** (16 C / 16 T CPU, 128 GB memory, up to 15 Gbps network): 111 seconds
* **32 vCPU Instances**
  * **c8g.8xlarge** (32 C / 32 T CPU, 64 GB memory, 15 Gbps network): 91 seconds
  * **c8gn.8xlarge** (32 C / 32 T CPU, 64 GB memory, 100 Gbps network): 67 seconds
  * **m8g.8xlarge** (32 C / 32 T CPU, 128 GB memory, 15 Gbps network): 77 seconds
  * **r8g.8xlarge** (32 C / 32 T CPU, 256 GB memory, 15 Gbps network): 74 seconds
* **48 vCPU Instances**
  * **c8g.12xlarge** (48 C / 48 T CPU, 96 GB memory, 22.5 Gbps network): 64 seconds
  * **c8gn.12xlarge** (48 C / 48 T CPU, 96 GB memory, 150 Gbps network): 60 seconds
  * **m8g.12xlarge** (48 C / 48 T CPU, 192 GB memory, 22.5 Gbps network): 61 seconds
  * **r8g.12xlarge** (48 C / 48 T CPU, 384 GB memory, 22.5 Gbps network): 59 seconds
* **64 vCPU Instances***
  * **c8g.16xlarge** (64 C / 64 T CPU, 128 GB memory, 30 Gbps network): 54 seconds
  * **c8gn.16xlarge** (64 C / 64 T CPU, 128 GB memory, 200 Gbps network): 53 seconds
  * **m8g.16xlarge** (64 C / 64 T CPU, 256 GB memory, 30 Gbps network): 54 seconds
  * **r8g.16xlarge** (64 C / 64 T CPU, 512 GB memory, 30 Gbps network): 54 seconds
* **96 vCPU Instances**
  * **c8g.24xlarge** (96 C / 96 T CPU, 192 GB memory, 40 Gbps network): 50 seconds
  * **c8gn.24xlarge** (96 C / 96 T CPU. 192 GB memory, 300 Gbps network): 49 seconds
  * **m8g.24xlarge** (96 C / 96 T CPU, 384 GB memory, 40 Gbps network): 50 seconds
  * **r8g.24xlarge** (96 C / 96 T CPU, 768 GB memory, 40 Gbps network): 50 seconds
