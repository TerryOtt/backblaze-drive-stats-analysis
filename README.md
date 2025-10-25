# backblaze-drive-stats-analysis

**Updated 2025-10-25**

## Creating Quarterly AFR CSV

**NOTE**: 64 GB of memory is _required_, otherwise process will be killed by the OS ue to memory exhaustion.

Candidate AWS EC2 instances (just helpful references, runs fine anywhere):

* c8g.8xlarge (16 CPU, 64 GB): 40 seconds
* m4g.4xlarge (8 CPU, 64 GB): 60 seconds
* r8g.2xlarge (4 CPU, 64 GB): 90 seconds

Runtime tests were done from AWS `us-west-1` region as `us-west-1` has the lowest latency to the Backblaze 
region `us-west-004` where the Backblaze B2 bucket hosting the drive stats data residess.

```
$ apt-get -y install python3-venv sudo build-essential python3-dev
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip3 install -r requirements.txt
$ python3 afr_viz_csv_from_iceberg.py drives_of_interest_regexes.json [s3_access_key] [s3_secret_access_key] quarterly_afr_2025q3.csv

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
