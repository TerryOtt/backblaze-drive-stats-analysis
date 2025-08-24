# backblaze-drive-stats-analysis

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
* There is a point of diminishing returns above 4-8 cores/8-16 threads
* **DO NOT ATTEMPT** on a system with less than 4 GB of RAM; you have been warned!

**Query time by AWS EC2 instance type**:

* **t3.small**: _system goes unresponsive; query never returns_
* **t3.medium**: 130 seconds
* **c7i.large**: 108 seconds
* **c7i.xlarge**: 53 seconds
* **c7i.2xlarge**: 30 seconds
* **c7i.4xlarge**: 19 seconds
* **c7i.8xlarge**: 15 seconds
* **c7i.12xlarge**: 13 seconds
* **c7i.16xlarge**: 11 seconds
* **c7i.24xlarge**: 11 seconds

## Calculate Per-Drive-Model AFR Stats 

```bash
$ python3 compute_afr.py iceberg_latest.csv backblaze-drive-stats-afr-by-model.csv
```
