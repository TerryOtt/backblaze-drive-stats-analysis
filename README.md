# backblaze-drive-stats-analysis

## Launch Trino and Register Table

Run commands to launch Trino and register the `drivestats` Iceberg table 
using instructions from the 
[backblaze-drive-stats-trino-iceberg](https://github.com/TerryOtt/backblaze-drive-stats-trino-iceberg/tree/main) repo.

## Pull Drive Stats

```bash
$ ./retrieve_backblaze_drive_data.sh > iceberg_latest.csv

$ ls -l iceberg_latest.csv
-rw-rw-r-- 1 tdo tdo 9062939 Aug 23 20:05 iceberg_latest.csv

$ wc -l iceberg_latest.csv
262961 iceberg_latest.csv
```

### Query Execution Time

The amount of time it takes to query the Iceberg table is 
_heavily_ dependent on the amount of compute resources available.

**Query time by AWS EC2 instance type**:

* **c7i.large**: 108 seconds
* **c7i.xlarge**: 53 seconds
* **c7i.2xlarge**: 
* **c7i.4xlarge**:
* **c7i.8xlarge**:
* **c7i.12xlarge**:

## Calculate Per-Drive-Model AFR Stats 

```bash
$ python3 compute_afr.py iceberg_latest.csv backblaze-drive-stats-afr-by-model.csv
```
