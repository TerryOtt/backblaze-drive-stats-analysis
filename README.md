# backblaze-drive-stats-analysis

## Supported CPU Architectures

Ubuntu 24.04 LTS, Trino, and Python all run great on both x86-64 and ARM64 (aka "aarch64") architectures. 
I have a preference for ARM instances when possible, as they are roughly as performant as x86 yet cheaper.

## Launch Trino and Register Table

Run commands to launch Trino and register the `drivestats` Iceberg table 
using instructions from the 
[backblaze-drive-stats-trino-iceberg](https://github.com/TerryOtt/backblaze-drive-stats-trino-iceberg/tree/main) repo.

## Retrieve Drive Stats From Iceberg Table

```bash
$ ./retrieve_backblaze_drive_data.sh > iceberg_latest.csv

$ ls -l iceberg_latest.csv
-rw-rw-r-- 1 tdo tdo 9062939 Aug 23 20:05 iceberg_latest.csv

$ wc -l iceberg_latest.csv
262961 iceberg_latest.csv
```

### Query Execution Time

The amount of time it takes to query the Iceberg table is 
_heavily_ correlated to compute resources. 

_Notes_:
* There are sharply diminishing returns above a CPU with 8 cores/16 threads
* **DO NOT ATTEMPT** on a system with less than 4 GB of RAM; you have been warned!
* Reducing network latency between your Trino instance and the S3 endpoint in Northern California reduces overall query time
  * S3 endpoint: `s3.us-west-004.backblazeb2.com`

**Query time by AWS EC2 instance type**:

EC2 instances were launched in the **`us-west-1`** (N. California) AWS region, due to its geographic proximity to the S3 endpoint.

* **t4g.small**: _system goes unresponsive; query never returns_
* **t4g.medium**: 106 seconds
* **c8g.large**: 84 seconds
* **c8g.xlarge**: 46 seconds
* **c8g.2xlarge**: 24 seconds
* **c8g.4xlarge**: 17 seconds
* **c8g.8xlarge**: 11 seconds
* **c8g.12xlarge**: 10 seconds

## Calculate Per-Drive-Model AFR Stats 

```bash
$ python3 compute_afr.py iceberg_latest.csv backblaze-drive-stats-afr-by-model.csv

real    0m1.423s
user    0m1.362s
sys     0m0.060s

$
```
