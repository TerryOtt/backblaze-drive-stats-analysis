import argparse
import datetime
import json
import pathlib
import polars
import re
import time
import xlsxwriter

import iceberg_table


type AfrPerDriveModelQuarterType = dict[str, dict[str, list[dict[str, str | int | float]]]]

def _parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description="Create quarterly AFR visualization CSV")

    default_min_drives: int = 2_000
    parser.add_argument('--min-drives', help="Minimum number of deployed drives for model, default: " +
                        f"{default_min_drives:,}",
                        type=int, default=default_min_drives)

    default_s3_endpoint: str = "https://s3.us-west-004.backblazeb2.com"
    default_b2_bucket_name: str = "drivestats-iceberg"
    default_b2_region: str = "us-west-004"
    default_table_path: str = "drivestats"

    parser.add_argument("--s3-endpoint",
                        default=default_s3_endpoint,
                        help=f"S3 Endpoint (default: \"{default_s3_endpoint}\")")

    parser.add_argument("--b2-region",
                        default=default_b2_region,
                        help=f"B2 Region (default: \"{default_b2_region}\")")

    parser.add_argument("--bucket-name",
                        default=default_b2_bucket_name,
                        help=f"B2 Bucket Name (default: \"{default_b2_bucket_name}\")")

    parser.add_argument("--table-path",
                        default=default_table_path,
                        help=f"B2 Bucket Table Path (default: \"{default_table_path}\")")

    parser.add_argument('drive_patterns_json', help='Path to JSON with drive regexes')

    #parser.add_argument("input_parquet_file", help="Path to parquet file we read from")

    parser.add_argument("b2_access_key",
                        help="Backblaze B2 Access Key")
    parser.add_argument("b2_secret_access_key",
                        help="Backblaze B2 Secret Access Key")

    parser.add_argument("output_xlsx", help="Path to output visualization XLSX file (s3:// supported)")
    return parser.parse_args()


def _normalize_drive_model_name(raw_drive_model: str) -> str:
    # Tokenize to see if we have manufacturer -- split with no params uses multi-whitespace as separator,
    #   so we get some nice trim and whitespace collapse
    model_tokens: list[str] = raw_drive_model.split()

    if not 1 <= len(model_tokens) <= 2:
        raise ValueError(f"Drive model name '{raw_drive_model}' did not result in 1 or 2 tokens")

    models_to_mfrs: dict[str, str] = {
        r'ST\d+'    : 'Seagate',
        r'WU[HS]72' : 'WDC/HGST',
    }

    expected_mfr_strings: set[str] = {
        'WDC/HGST',
        'Seagate',
        'Toshiba',
        'WDC',
    }

    # Figure out the manufacturer if there wasn't on00e
    if len(model_tokens) == 1:
        for curr_regex in models_to_mfrs:
            if re.match(curr_regex, model_tokens[0]):
                return f"{models_to_mfrs[curr_regex]} {model_tokens[0]}"

        # If we get here, we didn't get a match and puke out
        raise ValueError(f"Cannot determine mfr from model string: {raw_drive_model}")

    # Two token cases

    # Do some mfr name mappings
    name_mappings: dict[str, str] = {
        "TOSHIBA"   : "Toshiba",
        "HGST"      : "WDC/HGST",
        "WDC"       : "WDC/HGST",
    }
    if model_tokens[0] in name_mappings:
        model_tokens[0] = name_mappings[model_tokens[0]]

    if model_tokens[0] not in expected_mfr_strings:
        raise ValueError(f"Drive mfr {model_tokens[0]} not recognized")

    normalized_drive_model_name: str = " ".join(model_tokens)

    return normalized_drive_model_name


def _source_lazyframe(args: argparse.Namespace) -> polars.LazyFrame:
    print("\nOpening Polars datasource...")

    # Let's try some Iceberg magic
    current_iceberg_schema_uri: str = iceberg_table.current_metadata_file_s3_uri(
        args.b2_access_key,
        args.b2_secret_access_key,
        args.s3_endpoint,
        args.bucket_name,
        args.table_path
    )

    base_filename: str = pathlib.Path(current_iceberg_schema_uri).name

    print(f"\tCurrent Backblaze Drive Stats Iceberg schema file: {base_filename}")
    print(f"\t\tSchema URI: {current_iceberg_schema_uri}")

    storage_options = {
        "s3.endpoint"           : args.s3_endpoint,
        "s3.region"             : args.b2_region,
        "s3.access-key-id"      : args.b2_access_key,
        "s3.secret-access-key"  : args.b2_secret_access_key,
    }

    source_lazyframe: polars.LazyFrame = polars.scan_iceberg(current_iceberg_schema_uri, 
                                                             storage_options=storage_options)

    return source_lazyframe


def _increment_datetime_one_quarter(dt: datetime.date) -> datetime.date:
    incremented_year: int
    incremented_month: int = (dt.month + 3) % 12
    if incremented_month == 1:
        incremented_year = dt.year + 1
    else:
        incremented_year = dt.year

    return datetime.date(incremented_year, incremented_month, 1)


def _afr_calc(cumulative_drive_days: int, cumulative_drive_failures: int) -> float:
    # Scaling factor is 365 unit-days / year
    afr_scaling_factor: float = 365.0

    annualized_failure_rate_percent: float = ( float(cumulative_drive_failures) / float(cumulative_drive_days) ) * \
                                             afr_scaling_factor * 100.0

    return annualized_failure_rate_percent


def _create_normalized_model_name_series( drive_models_name_smart_series: polars.Series ) -> polars.Series:
    normalized_names: list[str] = []
    for smart_model_name in drive_models_name_smart_series:
        normalized_names.append(_normalize_drive_model_name(smart_model_name))
    model_names_normalized_series: polars.Series = polars.Series("model_name_normalized", normalized_names)

    return model_names_normalized_series


def _get_smart_drive_model_mappings(smart_drive_model_names_series: polars.Series) -> polars.DataFrame:
    print("\nETL pipeline stage 2 of 4: Create mapping table for SMART model name -> normalized model name...")

    smart_drive_model_mappings_df: polars.DataFrame = smart_drive_model_names_series.to_frame()

    # Add column with normalized drive model name
    smart_drive_model_mappings_df: polars.DataFrame = smart_drive_model_mappings_df.with_columns(
        polars.col("drive_model_name_smart").map_batches(_create_normalized_model_name_series,
                                                         return_dtype=polars.String).alias(
            "drive_model_name_normalized")
    )

    normalized_drive_model_name_count: int = smart_drive_model_mappings_df.get_column(
        "drive_model_name_normalized" ).unique().len()

    print(f"\t{smart_drive_model_names_series.len()} SMART drive model names -> {normalized_drive_model_name_count} "
        "normalized drive model names" )

    return smart_drive_model_mappings_df


def _get_smart_drive_model_names(args: argparse.Namespace,
                                original_source_lazyframe: polars.LazyFrame) -> polars.Series:

    print("\nETL pipeline stage 1 of 4: Retrieve candidate SMART drive model names...")

    with open(args.drive_patterns_json, "r") as json_handle:
        drive_model_patterns: list[str] = json.load(json_handle)
    print(f"\tRetrieved {len(drive_model_patterns):,} regexes for SMART drive model names from "
        f"\"{args.drive_patterns_json}\"")

    multi_regex_pattern: str = "|".join(drive_model_patterns)
    # print(f"multi regex pattern: {multi_regex_pattern}")

    # We want all unique drive model names found in the source file which match one of the drive model regexes
    operation_start: float = time.perf_counter()
    print("\tRetrieving unique candidate SMART drive model names from Polars...")
    drive_models_smart_series: polars.Series = original_source_lazyframe.filter(
        polars.col("model").str.contains(multi_regex_pattern)
    ).select(
        "model"
    ).collect().get_column("model").unique().sort().rename("drive_model_name_smart")
    operation_end: float = time.perf_counter()
    operation_duration: float = operation_end - operation_start

    # How many unique drive models and how much time?
    print( f"\t\tRetrieved {len(drive_models_smart_series):,} candidate SMART drive model names in "
        f"{operation_duration:.01f} seconds")

    return drive_models_smart_series


def _do_quarterly_afr_calculations(
        args: argparse.Namespace,
        source_lazyframe: polars.LazyFrame,
        smart_model_name_mappings_dataframe: polars.DataFrame ) -> AfrPerDriveModelQuarterType:

    print("\nETL pipeline stage 3 of 4: Perform AFR calculations...")

    operation_start: float = time.perf_counter()

    quarterly_afr_calc_data: polars.DataFrame = source_lazyframe.join(
        smart_model_name_mappings_dataframe.lazy(),
        left_on="model",
        right_on="drive_model_name_smart",
    ).group_by(
        polars.col("drive_model_name_normalized").alias("model_name"),
        polars.col("date").dt.year().alias("year"),
        polars.col("date").dt.quarter().alias("quarter")
    ).agg(
        polars.col("failure").sum().alias("qtr_failure_count"),
        polars.col("failure").count().alias("qtr_drive_days"),
        polars.col("serial_number").unique().len().alias("qtr_unique_drives_deployed"),
    ).select(
        "model_name",
        "year",
        "quarter",
        "qtr_unique_drives_deployed",
            "qtr_drive_days",
        "qtr_failure_count"
    ).collect().sort(
        "model_name",
        "year",
        "quarter"
    )

    cumulative_quarter_stats: dict[str, dict[str, dict[str, str | int]]] = {}
    afr_by_mfr_model_quarter: AfrPerDriveModelQuarterType = {}

    for curr_quarter_data in quarterly_afr_calc_data.iter_rows(named=True):
        curr_manufacturer, curr_drive_model = curr_quarter_data['model_name'].split()
        year_quarter: str = f"{curr_quarter_data['year']} Q{curr_quarter_data['quarter']}"
        # print(f"\tMfr: {curr_manufacturer}, model: {curr_drive_model}, qtr: {year_quarter}")

        if curr_manufacturer not in cumulative_quarter_stats:
            cumulative_quarter_stats[curr_manufacturer]: dict[str, dict[str, int]] = {}
            afr_by_mfr_model_quarter[curr_manufacturer] = {}
        if curr_drive_model not in cumulative_quarter_stats[curr_manufacturer]:
            cumulative_quarter_stats[curr_manufacturer][curr_drive_model]: dict[str, str | int] = {
                'cumulative_drive_days'     : 0,
                'cumulative_failure_count'  : 0,
            }
            afr_by_mfr_model_quarter[curr_manufacturer][curr_drive_model] = []

        curr_mfr_model_stats: dict[str, str | int] = cumulative_quarter_stats[curr_manufacturer][curr_drive_model]

        curr_mfr_model_stats['cumulative_drive_days'] += curr_quarter_data['qtr_drive_days']
        curr_mfr_model_stats['cumulative_failure_count'] += curr_quarter_data['qtr_failure_count']

        # If this quarter has enough drives deployed, add new quarter of AFR data
        if curr_quarter_data['qtr_unique_drives_deployed'] >= args.min_drives:
            afr_by_mfr_model_quarter[curr_manufacturer][curr_drive_model].append(
                {
                    'year_quarter'              : year_quarter,
                    'unique_drives_deployed'    : curr_quarter_data['qtr_unique_drives_deployed'],
                    'afr'                       : _afr_calc( curr_mfr_model_stats['cumulative_drive_days'],
                                                             curr_mfr_model_stats['cumulative_failure_count'] ),
                }
            )

    del cumulative_quarter_stats

    # Purge any models with no quarterly data
    for curr_mfr in sorted(afr_by_mfr_model_quarter):

        # The list is to get it so we iterate over a list rather than the dict and can delete keys from the dict
        for curr_model in sorted(list(afr_by_mfr_model_quarter[curr_mfr])):

            # If the list of quarterly stats is empty (hence falsy), remove this model from the data dict
            if not afr_by_mfr_model_quarter[curr_mfr][curr_model]:
                del afr_by_mfr_model_quarter[curr_mfr][curr_model]

    # print(json.dumps(afr_by_mfr_model_quarter, indent=4, sort_keys=True))

    operation_duration: float = time.perf_counter() - operation_start
    print(f"\tOperation time: {operation_duration:.01f} seconds")

    return afr_by_mfr_model_quarter


def _xlsx_add_header_rows(afr_by_mfr_model_qtr: AfrPerDriveModelQuarterType,
                          excel_workbook: xlsxwriter.workbook.Workbook,
                          excel_sheet: xlsxwriter.workbook.Worksheet ) -> None:

    # Generate the first five header rows

    # Row 1: |      |     |                                                             Drives
    # Row 2: |      |     |                              Mfr 1                            |  ... Mfr N
    # Row 3: |      |     |         Mfr 1 Drive 1         |         Mfr 1 Drive 2         |
    # Row 4: |      |     |      AFR      | Deploy Count  |      AFR      | Deploy Count  |
    # Row 5: | Year | Qtr | Value | Delta | Value | Delta | Value | Delta | Value | Delta |

    bottom_center_bold_merge_format: xlsxwriter.workbook.Format = excel_workbook.add_format(
        {
            'bold'      : True,
            'border'    : 1,
            'align'     : 'center',
            'valign'    : 'vbottom',
        }
    )

    vcenter_center_bold_merge_format: xlsxwriter.workbook.Format = excel_workbook.add_format(
        {
            'bold'      : True,
            'border'    : 1,
            'align'     : 'center',
            'valign'    : 'vcenter',
        }
    )

    # Compute values like total drives being displayed and drives for each manufacturer
    total_model_count: int = 0
    drive_models_per_mfr: dict[str, int] = {}
    for curr_mfr in sorted(afr_by_mfr_model_qtr):
        for _ in sorted(list(afr_by_mfr_model_qtr[curr_mfr])):
            # Already removed models with no quarters >= min drives
            total_model_count += 1

            if curr_mfr not in drive_models_per_mfr:
                drive_models_per_mfr[curr_mfr] = 0
            drive_models_per_mfr[curr_mfr] += 1

    # Year (A1:A5)
    excel_sheet.merge_range(
        'A1:A5',
        'Year',
        bottom_center_bold_merge_format
    )

    # Quarter (B1:B5)
    excel_sheet.merge_range(
        'B1:B5',
        'Qtr',
        bottom_center_bold_merge_format
    )

    # Drives (C1 : C$1 ... (column that is total number drives * 4) - 1 $1
    colspan_drives: int = (4 * total_model_count) - 1
    excel_sheet.merge_range(
        0, 2, 0, 2 + colspan_drives,
        'Drive Data',
        vcenter_center_bold_merge_format
    )

    # Create cells for all the mfrs along row 2
    curr_col: int = 2
    for curr_mfr in sorted(drive_models_per_mfr):
        cols_for_this_mfr: int = drive_models_per_mfr[curr_mfr] * 4
        excel_sheet.merge_range(
            1, curr_col, 1, curr_col + cols_for_this_mfr - 1,
            curr_mfr,
            vcenter_center_bold_merge_format
        )

        curr_col += cols_for_this_mfr

    # Create row of drive models for each mfr
    curr_col = 2
    for curr_mfr in sorted(afr_by_mfr_model_qtr):
        for curr_model in sorted(afr_by_mfr_model_qtr[curr_mfr]):
            excel_sheet.merge_range(
                2, curr_col, 2, curr_col + 3,
                curr_model,
                vcenter_center_bold_merge_format
            )
            curr_col += 4

    # Write "AFR" and "Deploy Count" for each drive model
    curr_col = 2
    for _ in range(total_model_count):
        excel_sheet.merge_range(
            3, curr_col, 3, curr_col + 1,
            "AFR",
            vcenter_center_bold_merge_format
        )
        excel_sheet.merge_range(
            3, curr_col + 2, 3, curr_col + 3,
            "Deploy Count",
            vcenter_center_bold_merge_format
        )
        curr_col += 4

    # Add alternating cells for Value and Delta for all cells
    curr_col = 2

    # Each drive model gets two sets of Value/Delta, one for AFR, one for Deploy Count
    for _ in range(total_model_count * 2):
        excel_sheet.write(4, curr_col, "Value", vcenter_center_bold_merge_format )
        excel_sheet.write(4, curr_col + 1, "Delta", vcenter_center_bold_merge_format)
        curr_col += 2

    print(f"\tHeader rows added to sheet")


def _xlsx_add_data_rows(afr_by_mfr_model_qtr: AfrPerDriveModelQuarterType,
                        max_num_data_rows: int,
                        excel_workbook: xlsxwriter.workbook.Workbook,
                        excel_sheet: xlsxwriter.workbook.Worksheet ) -> tuple[int, int]:

    curr_year_quarter: tuple[int, int]
    max_year_quarter: tuple[int, int] = (0, 0)

    curr_col: int = 2
    curr_row: int

    # Create non-bold centered format
    float_format: xlsxwriter.workbook.Format = excel_workbook.add_format(
        {
            'align'         : 'center',
            'valign'        : 'vcenter',
            'border'        : 1,
            'num_format'    : '0.000',
        }
    )

    int_format: xlsxwriter.workbook.Format = excel_workbook.add_format(
        {
            'align'         : 'center',
            'valign'        : 'vcenter',
            'border'        : 1,
            'num_format'    : '#,##0',
        }
    )

    blank_with_border_format: xlsxwriter.workbook.Format = excel_workbook.add_format(
        {
            'border': 1,
        }
    )

    for curr_mfr in sorted(afr_by_mfr_model_qtr):
        for curr_model in sorted(afr_by_mfr_model_qtr[curr_mfr]):
            curr_year_quarter = (1, 1)
            prev_model_quarter_values: tuple[float, int] = (0.0, 0)
            curr_row = 5
            while afr_by_mfr_model_qtr[curr_mfr][curr_model]:
                # Remove first entry and display it
                display_data: dict[str, int | float | str] = afr_by_mfr_model_qtr[curr_mfr][curr_model].pop(0)

                # AFR Value
                excel_sheet.write(curr_row, curr_col, display_data['afr'], float_format)

                # AFR Delta
                excel_sheet.write(curr_row, curr_col + 1,
                                  display_data['afr'] - prev_model_quarter_values[0],
                                  float_format)

                # Deploy Count Value
                excel_sheet.write(curr_row, curr_col + 2, display_data['unique_drives_deployed'],
                                  int_format)

                # Deploy Count Delta
                excel_sheet.write(curr_row, curr_col + 3,
                                  display_data['unique_drives_deployed'] - prev_model_quarter_values[1],
                                  int_format)

                # Update values for prev qtr
                prev_model_quarter_values = (display_data['afr'], display_data['unique_drives_deployed'])

                # Update max year quarter if current is bigger
                if (
                        (curr_year_quarter[0] > max_year_quarter[0]) or

                        (
                            (curr_year_quarter[0] == max_year_quarter[0]) and
                            (curr_year_quarter[1] > max_year_quarter[1])
                        )
                ):
                    # Bump up max year quarter to current
                    max_year_quarter = curr_year_quarter

                # Increment current year & quarter
                if curr_year_quarter[1] < 4:
                    curr_year_quarter = (curr_year_quarter[0], curr_year_quarter[1] + 1)
                else:
                    curr_year_quarter = (curr_year_quarter[0] + 1, 1)

                # Increment display row
                curr_row += 1

            # Handle any rows from here to max
            for curr_row in range(curr_row, max_num_data_rows + 5):
                for col_offset in range(4):
                    excel_sheet.write(curr_row, curr_col + col_offset, None, blank_with_border_format)

            # Increment display column four to move to next model
            curr_col += 4


def _xlsx_add_year_quarter_rows(num_data_rows: int,
                                excel_workbook: xlsxwriter.workbook.Workbook,
                                excel_sheet: xlsxwriter.workbook.Worksheet) -> None:
    year_quarter_format: xlsxwriter.workbook.Format = excel_workbook.add_format(
        {
            'align': 'center',
            'valign': 'vcenter',
            'border': 1,
        }
    )

    curr_year: int = 1
    curr_quarter: int = 1

    for curr_row in range(num_data_rows):
        excel_sheet.write(curr_row + 5, 0, curr_year, year_quarter_format)
        excel_sheet.write(curr_row + 5, 1, curr_quarter, year_quarter_format)

        curr_quarter += 1

        # Did we roll to next year?
        if curr_quarter == 5:
            curr_year += 1
            curr_quarter = 1


def _get_max_data_row_count(quarterly_afr_by_drive_model: AfrPerDriveModelQuarterType) -> int:
    max_data_rows: int = 0
    for curr_mfr in sorted(quarterly_afr_by_drive_model):
        for curr_model in sorted(quarterly_afr_by_drive_model[curr_mfr]):
            max_data_rows = max(max_data_rows, len(quarterly_afr_by_drive_model[curr_mfr][curr_model]))

    return max_data_rows


def _generate_output_xlsx(args: argparse.Namespace,
                          quarterly_afr_by_drive_model: AfrPerDriveModelQuarterType ) -> str:

    print("\nETL pipeline stage 4 of 4: Generating XLSX for visualizing Backblaze drive stats AFR data...")

    generated_xlsx_path: str

    # Find out if we are generating a temp file that gets copied to S3, or a filesystem URL
    if args.output_xlsx.startswith("s3://"):
        # generated_xlsx_path = (create temp file name)
        raise NotImplementedError("S3 URL paths for XLSX not implemented yet")
    else:
        generated_xlsx_path = args.output_xlsx

    print(f"\tOutput XLSX path: {generated_xlsx_path}")

    max_data_row_count: int = _get_max_data_row_count(quarterly_afr_by_drive_model)

    with xlsxwriter.Workbook(generated_xlsx_path) as excel_workbook:
        excel_sheet: xlsxwriter.workbook.Worksheet = excel_workbook.add_worksheet()
        _xlsx_add_header_rows(quarterly_afr_by_drive_model, excel_workbook, excel_sheet)
        _xlsx_add_year_quarter_rows(max_data_row_count, excel_workbook, excel_sheet)
        _xlsx_add_data_rows(quarterly_afr_by_drive_model, max_data_row_count, excel_workbook, excel_sheet)

    return generated_xlsx_path


def _main() -> None:

    processing_start: float = time.perf_counter()

    args: argparse.Namespace = _parse_args()
    original_source_lazyframe: polars.LazyFrame = _source_lazyframe(args)

    smart_drive_model_names: polars.Series = _get_smart_drive_model_names(args, original_source_lazyframe)

    smart_model_name_mappings_dataframe: polars.DataFrame = _get_smart_drive_model_mappings(smart_drive_model_names)

    # Can delete SMART drive model name series as its no longer used
    del smart_drive_model_names

    afr_by_mfr_model_quarter: AfrPerDriveModelQuarterType = _do_quarterly_afr_calculations(
        args, original_source_lazyframe, smart_model_name_mappings_dataframe )

    generated_xlsx_file_path: str = _generate_output_xlsx(args, afr_by_mfr_model_quarter )

    processing_duration: float = time.perf_counter() - processing_start
    print(f"\nETL pipeline total processing time: {processing_duration:.01f} seconds\n")


if __name__ == "__main__":
    _main()
