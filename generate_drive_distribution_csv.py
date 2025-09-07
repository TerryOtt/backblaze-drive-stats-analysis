import argparse
import csv
import datetime
import json
import re


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert Trino CSV to quarterly drive distribution CSV")
    min_drives_default: int = 1000
    parser.add_argument("--min-drives", type=int, default=min_drives_default,
                        help=f"Minimum number of drives for model to be included (default: {min_drives_default})")
    parser.add_argument("trino_csv", help="Path to CSV with raw data from Trino")
    parser.add_argument("drive_model_regex_json", help="Path to JSON file with drive model regexes")
    parser.add_argument("drive_distribution_csv", help="Path to output CSV")

    return parser.parse_args()


def _read_drives_of_interest_regexes(args: argparse.Namespace) -> list[str]:
    with open(args.drive_model_regex_json, 'r' ) as regex_handle:
        regexes: list[str] = json.load(regex_handle)
    return regexes


def _clean_drive_model_str(drive_model_str: str) -> str:
    """Clean up whitespace and deal with inconsistent model names.
    :param drive_model_str: Drive model string from CSV
    :return: model string with all extra whitespace removed
    """
    clean_str: str = re.sub(r'\s+', ' ', drive_model_str.strip())
    model_string_tokens: list[str] = clean_str.split(' ')

    # If there are multiple tokens and the final token matches a drive of interest,
    #    use the final token as the drive model as the data isn't consistent.
    #
    # Example: raw data includes both "WUH721816ALE6L4" and "WDC WUH721816ALE6L4" models,
    #          which are obviously the same thing. WOMP WOMP.
    if len(model_string_tokens) > 1:
        # Compare last token to regular expressions that are drives of interest
        #   If the regular expression matches, use final token as drive model
        drive_models_of_interest_patterns: tuple[str, ...] = (
            r'[HW]UH72\d+',
            r'ST\d+NM',
            r'MG\d{2}',
        )

        # re.match only checks for matches at the start of the string, hence no need for ^ or \b
        for curr_pattern in drive_models_of_interest_patterns:
            if re.match(curr_pattern, model_string_tokens[-1]):
                return model_string_tokens[-1]

    return clean_str


def _get_daily_drive_distribution(args: argparse.Namespace, drives_of_interest_regex: list[str]):
    daily_drive_distribution: dict[str, dict[str, int]] = {}
    with open(args.trino_csv, 'r') as trino_handle:
        csv_reader: csv.DictReader = csv.DictReader(trino_handle)
        
        for curr_csv_row in csv_reader:
            cleaned_drive_model: str = _clean_drive_model_str(curr_csv_row['model'])
            drive_of_interest: bool = False
            for curr_pattern in drives_of_interest_regex:
                if re.match(curr_pattern, cleaned_drive_model):
                    drive_of_interest = True
                    break

            if not drive_of_interest:
                #print(f"Ignoring model {cleaned_drive_model}, matched no patterns")
                continue

            if curr_csv_row['date'] not in daily_drive_distribution:
                daily_drive_distribution[curr_csv_row['date']] = {}

            if cleaned_drive_model not in daily_drive_distribution[curr_csv_row['date']]:
                daily_drive_distribution[curr_csv_row['date']][cleaned_drive_model] = 0

            daily_drive_distribution[curr_csv_row['date']][cleaned_drive_model] += int(
                curr_csv_row['model_count'])

    return daily_drive_distribution


def _aggregate_data(daily_data: dict[str, dict[str, int]]) -> list[dict[str, str|dict[str, int]]]:
    # Find starting date in data
    sorted_dates: list[str] = sorted(daily_data)
    first_day_str = sorted_dates[0]
    last_day_str = sorted_dates[-1]
    starting_date:datetime.date = datetime.date.fromisoformat(first_day_str)
    last_day_of_data: datetime.date = datetime.date.fromisoformat(last_day_str)
    full_quarterly_stats: list[dict[str, str|dict[str, int]]] = []
    while starting_date < last_day_of_data:
        quarter_str: str = ""
        if starting_date.month <= 3:
            ending_date = datetime.date.fromisoformat( f"{starting_date.year}-03-31")
            quarter_str = "1"
        elif starting_date.month <= 6:
            ending_date = datetime.date.fromisoformat( f"{starting_date.year}-06-30")
            quarter_str = "2"
        elif starting_date.month <= 9:
            ending_date = datetime.date.fromisoformat( f"{starting_date.year}-09-30")
            quarter_str = "3"
        else:
            ending_date = datetime.date.fromisoformat( f"{starting_date.year}-12-31")
            quarter_str = "4"
        
        #print(f"\nAggregation date range: {starting_date.isoformat()} - {ending_date.isoformat()}")

        quarterly_stats = {
            "calendar_quarter": f"{starting_date.year} Q{quarter_str}",
            "drives": {},
        }

        curr_date: str = starting_date.isoformat()
        while curr_date != ending_date.isoformat():
            curr_date = sorted_dates.pop(0)

            # Doing MAX aggregation, value is most of that drive we've seen in the quarter
            data_to_aggregate = daily_data[curr_date]
            for drive_model in data_to_aggregate:
                if drive_model not in quarterly_stats['drives']:
                    quarterly_stats['drives'][drive_model] = 0
            
                quarterly_stats['drives'][drive_model] = max(
                    quarterly_stats['drives'][drive_model], data_to_aggregate[drive_model])

        full_quarterly_stats.append(quarterly_stats)

        starting_date = ending_date + datetime.timedelta(days=1)

    return full_quarterly_stats


def _apply_min_drive_filter(args: argparse.Namespace,
                            aggregated_data: list[dict[str, str|dict[str, int]]]
) -> list[dict[str, str|dict[str, int]]]:

    # Find max drives within all quarters for all drives
    max_drives_by_drive: dict[str, int] = {}
    for curr_quarter in aggregated_data:
        for curr_drive in curr_quarter['drives']:
            if curr_drive not in max_drives_by_drive:
                max_drives_by_drive[curr_drive] = 0
            curr_quarter['drives'][curr_drive] = max(max_drives_by_drive[curr_drive],
                                                     curr_quarter['drives'][curr_drive])

    filtered_agg_data: list[dict[str, str|dict[str, int]]] = []
    for curr_quarter in aggregated_data:
        filtered_quarter: dict[str, str|dict[str, int]] = {
            'calendar_quarter': curr_quarter['calendar_quarter'],
            'drives': {},
        }
        for curr_drive in curr_quarter['drives']:
            if curr_quarter['drives'][curr_drive] >= args.min_drives:
                filtered_quarter['drives'][curr_drive] = curr_quarter['drives'][curr_drive]

        filtered_agg_data.append(filtered_quarter)

    return filtered_agg_data


def _get_sorted_drive_models(aggregated_data: list[dict[str, str | int]]) -> tuple[str, ...]:
    drives_set: set[str] = set()
    for quarterly_data in aggregated_data:
        for curr_drive in quarterly_data['drives']:
            if curr_drive not in drives_set:
                drives_set.add(curr_drive)

    return tuple(sorted(drives_set))


def _generate_viz_csv(args: argparse.Namespace,
                      sorted_drive_models: tuple[str, ...],
                      aggregated_data: list[dict[str, str|dict[str, int]]]) -> None:
    csv_fieldnames: list[str] = ["calendar_quarter"]
    csv_fieldnames.extend( sorted_drive_models )

    with open(args.drive_distribution_csv, "w") as csv_handle:
        output_csv: csv.DictWriter[str] = csv.DictWriter(csv_handle, fieldnames=csv_fieldnames)
        output_csv.writeheader()

        for curr_quarter_data in aggregated_data:

            data_row: dict[str, str | int] = {
                'calendar_quarter': curr_quarter_data['calendar_quarter'],
            }
            data_row.update(curr_quarter_data['drives'])

            output_csv.writerow(data_row)


def _main() -> None:
    args: argparse.Namespace = _parse_args()
    drives_of_interest_regex = _read_drives_of_interest_regexes(args)
    daily_drive_distribution: dict[str, dict[str, int]] = _get_daily_drive_distribution(
        args, drives_of_interest_regex)
    aggregated_data: list[dict[str, str|dict[str, int]]] = _aggregate_data(daily_drive_distribution)
    filtered_aggregated_data: list[dict[str, str|dict[str, int]]] = _apply_min_drive_filter(
        args, aggregated_data )
    sorted_drive_models: tuple[str, ...] = _get_sorted_drive_models(filtered_aggregated_data)
    _generate_viz_csv(args, sorted_drive_models, filtered_aggregated_data)


if __name__ == "__main__":
    _main()
