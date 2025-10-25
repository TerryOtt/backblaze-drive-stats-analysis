# backblaze-drive-stats-analysis

**Updated 2025-10-24**

## Reading Local Parquet File

```
$ (Get local Parquet data (backblaze-drive-stats-pyiceberg))
$ source .venv/bin/activate
$ python3 afr_viz_csv.py                  \
    drives_of_interest_regexes.json       \
    backblaze_drive_stats_2025q3.parquet  \
    quarterly_afr_2025q3.csv 

Polars datasource: local Parquet file, "backblaze_drive_stats_2025q3.parquet"

ETL pipeline stage 1 / 6: Retrieve candidate SMART drive model names...
	Retrieved 3 regexes for SMART drive model names from "drives_of_interest_regexes.json"
	Retrieving unique candidate SMART drive model names from Polars...
		Retrieved 38 candidate SMART drive model names in 5.4 seconds

ETL pipeline stage 2 / 6: Create mapping table for SMART model name -> normalized model name...
	38 SMART drive model names -> 34 normalized drive model names

ETL pipeline stage 3 / 6: Retrieve deploy counts for drive models...
	Retrieving drive deployment counts from Polars...
		Retrieved drive deploy counts for 19 candidate drive models in 30.9 seconds

	INFO: 15 candidate drive models were filtered out due to drive counts < 2,000 
		(modify with --min-drives)

ETL pipeline stage 4 / 6: Retrieve AFR calculation input data...
	Retrieving daily drive health data from Polars for 19 drive models...
	Reading incremental results batches, max rows per batch = 16,384 (modify with --max-batch)
	Retrieved 32,815 rows of drive health data from Polars
	Retrieved drive health data in 4.0 seconds

ETL pipeline stage 5 / 6: Perform AFR calculations...
	Quarterly AFR calculations completed

ETL pipeline stage 6 / 6: Writing AFR data to visualization CSV...
	Creating visualization CSV file "quarterly_afr_2025q3.csv"
	Max quarters of AFR data for any drive model: 33

ETL pipeline total processing time: 40.4 seconds

	
$ ls -laF quarterly_afr_2025q3.csv 
-rw-rw-r-- 1 ubuntu ubuntu 3326 Oct 20 20:39 quarterly_afr_2025q3.csv 

$ wc -l quarterly_afr_2025q3.csv 
34 quarterly_afr_2025q3.csv 
```
