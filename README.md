# backblaze-drive-stats-analysis

## Launch Trino and Register Table

Run commands to launch Trino and register the `drivestats` from the [backblaze-drive-stats-trino-iceberg repo](https://github.com/TerryOtt/backblaze-drive-stats-trino-iceberg/tree/main).

## Pull Drive Stats

```bash
$ time ./retrieve_backblaze_drive_data.sh > drive_stats.csv

real    1m34.775s
user    0m0.186s
sys     0m0.480s

$ ls -l drive_stats.csv
-rw-rw-r-- 1 tdo tdo 9062939 Aug 23 20:05 drive_stats.csv

$ wc -l drive_stats.csv
262961 drive_stats.csv
```

This was executed on an AWS `c7i-flex.large` EC2 instance
* **CPU**: 1 core/2 thread
* **Memory**: 4 GB

## Calculate Per-Drive-Model AFR Stats 

```bash
$ ./compute_afr.py drive_stats.csv backblaze-drive-model-daily-afr.csv
```
