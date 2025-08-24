# backblaze-drive-stats-analysis

## TL;DR -- Gimme The Data

Instructions for Ubuntu 24.04.

### Install Docker 

```bash
$ sudo apt-get update
$ sudo apt-get install -y apt-transport-https ca-certificates curl software-properties-common
$ sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
$ echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
$ sudo apt-get update
$ sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
$ sudo systemctl enable docker
$ sudo usermod -aG docker $USER
$ newgrp docker
```

### Start Trino and Hive Metastore Containers

```bash
$ mkdir ~/git
$ cd ~/git
$ git clone https://github.com/TerryOtt/backblaze-drive-stats-trino-iceberg.git
$ cd backblaze-drive-stats-trino-iceberg
$ docker compose up --detach

# Wait 30-40 seconds for containers to download and fully start
```

### Get Drive Data For AFR Calculations From Backblaze Iceberg Table

```bash
$ time ./register-drive-stats-table.sh
CREATE SCHEMA
USE
CALL

real    1m8.049s
user    0m0.134s
sys     0m0.057s

$ cd ..
$ git clone https://github.com/TerryOtt/backblaze-drive-stats-analysis.git
$ cd backblaze-drive-stats-analysis
$ time ./retrieve_backblaze_drive_data.sh > backblaze-drive-stats_`date +%F`.csv

real    1m11.701s
user    0m0.116s
sys     0m0.358s

$ du -h backblaze-drive-stats_`date +%F`.csv
8.7M    backblaze-drive-stats_YYYY-MM-DD.csv

$ wc -l backblaze-drive-stats_`date +%F`.csv
262961 backblaze-drive-stats_YYYY-MM-DD.csv
```

### Generate CSV File With Annualized Failure Rate (AFR) Data

```bash
$ time python3 compute_afr.py backblaze-drive-stats_`date +%F`.csv backblaze-drive-models-afr_`date +%F`.csv

real    0m1.451s
user    0m1.357s
sys     0m0.082s

$ du -h backblaze-drive-models-afr_`date +%F`.csv
12M     backblaze-drive-models-afr_YYYY-MM-DD.csv

$ wc -l backblaze-drive-models-afr_`date +%F`.csv
260728 backblaze-drive-models-afr_YYYY-MM-DD.csv

$ head -5 backblaze-drive-models-afr_`date +%F`.csv

drive_model,day_index,date,cumulative_drive_days,cumulative_drive_failures,annualized_failure_rate_percent
00MD00,1,2017-04-12,2,0,0.00
00MD00,2,2017-04-13,4,0,0.00
00MD00,3,2017-04-14,6,0,0.00
00MD00,4,2017-04-15,8,0,0.00

$
```

## Gruesome Detail

### Differing CSV Line Counts

Eagle-eyed users likely noticed something strange in the line count data above:

```bash
$ wc -l backblaze-drive-stats_`date +%F`.csv
262961 backblaze-drive-stats_YYYY-MM-DD.csv

$ wc -l backblaze-drive-models-afr_`date +%F`.csv
260728 backblaze-drive-models-afr_YYYY-MM-DD.csv
```

I'm sorry wut?

We should be **enriching** the Backblaze data rows with each day's cumulative AFR, not redacting any. 

Why did 2,233 rows of daily drive health data disappear?!?!?

```csv
$ tail -n +2 backblaze-drive-stats_YYYY-MM-DD.csv | cut -f 1 -d ',' | sort | uniq | grep "WUH721816ALE6L4"
WDC  WUH721816ALE6L4
WDC WUH721816ALE6L4
WUH721816ALE6L4

# Okay, they weren't consistent on the name of drive models in this data set, but they've been collecting
#   it for over a decade; mistakes are going to happen. 
#
# As long as there isn't more than one row of data for that drive per day, we should be fi--

$ grep "WUH721816ALE6L4.*,2024-11-20," backblaze-drive-stats_YYYY-MM-DD.csv
WDC WUH721816ALE6L4,2024-11-20,26395,0
WUH721816ALE6L4,2024-11-20,74,0

# ...Well. Dammit
```

**Ohhh no!**

There are multiple rows for the same drive model and date, because Backblaze weren't consistent on the 
drive model string for all drives -- even on the same daily stats collection run!

This means that the multiple rows for that day to be combined into one row. They aren't duplicate rows 
(thank goodness), but the drive counts and failure counts need to be added together for that day.

### Supported CPU Architectures

Ubuntu 24.04 LTS, Trino, and Python all run great on both x86-64 and ARM64 (aka "aarch64") architectures. 

I have a preference for ARM instances when possible, as they are roughly as performant as x86 yet cheaper.

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

### Network Latency To Backblaze S3 Endpoint

AWS `us-west-1` region is five network hops and one millisecond away from Backblaze's peering point at
[Equinix SV1](https://www.equinix.com/data-centers/americas-colocation/united-states-colocation/silicon-valley-data-centers/sv1)
in San Jose, California, USA.

```bash
$  traceroute s3.us-west-004.backblazeb2.com
traceroute to s3.us-west-004.backblazeb2.com (149.137.133.254), 30 hops max, 60 byte packets
 1  * 150.222.97.127 (150.222.97.127)  1.026 ms 150.222.97.131 (150.222.97.131)  1.013 ms
 2  240.0.168.1 (240.0.168.1)  1.202 ms 240.0.168.2 (240.0.168.2)  1.217 ms 240.0.168.0 (240.0.168.0)  1.214 ms
 3  242.2.27.69 (242.2.27.69)  1.747 ms 242.2.26.69 (242.2.26.69)  1.722 ms 242.2.27.195 (242.2.27.195)  0.977 ms
 4  52.93.70.185 (52.93.70.185)  1.109 ms 52.93.70.187 (52.93.70.187)  1.089 ms 52.93.70.203 (52.93.70.203)  0.969 ms
 5  eqix-sv1.backblaze-1.com (206.223.116.147)  1.072 ms  0.994 ms  1.068 ms
 6  149.137.143.67 (149.137.143.67)  40.965 ms  40.921 ms  40.938 ms
 7  * * *

$ whois 149.137.143.67

NetRange:       149.137.128.0 - 149.137.143.255
CIDR:           149.137.128.0/20
NetName:        BACKB-7
NetHandle:      NET-149-137-128-0-1
Parent:         NET149 (NET-149-0-0-0-0)
NetType:        Direct Allocation
OriginAS:
Organization:   Backblaze Inc (BACKB-7)
RegDate:        2020-09-10
Updated:        2025-03-25
Ref:            https://rdap.arin.net/registry/ip/149.137.128.0

OrgName:        Backblaze Inc
OrgId:          BACKB-7
Address:        201 Baldwin Ave.
City:           San Mateo
StateProv:      CA
PostalCode:     94401
Country:        US
RegDate:        2016-02-22
Updated:        2024-11-25
Ref:            https://rdap.arin.net/registry/entity/BACKB-7
```
