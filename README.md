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

## Network Latency To Backblaze S3 Endpoint

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
