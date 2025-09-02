import argparse
import csv
import datetime
import json
import re


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert Trino CSV to quarterly drive distribution CSV")
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
            for curr_pattern in drives_of_interest_regex:
                if re.match(curr_pattern, cleaned_drive_model):
                    if curr_csv_row['date'] not in daily_drive_distribution:
                        daily_drive_distribution[curr_csv_row['date']] = {}

                    if cleaned_drive_model not in daily_drive_distribution[curr_csv_row['date']]:
                         daily_drive_distribution[curr_csv_row['date']][cleaned_drive_model] = 0

                    daily_drive_distribution[curr_csv_row['date']][cleaned_drive_model] += int(curr_csv_row['model_count'])

    return daily_drive_distribution


def _aggregate_data(daily_data: dict[str, dict[str, int]]):
    # Find starting date in data
    sorted_dates: list[str] = sorted(daily_data)
    first_day_str = sorted_dates[0]
    last_day_str = sorted_dates[-1]
    starting_date:datetime.date = datetime.date.fromisoformat(first_day_str)
    last_day_of_data: datetime.date = datetime.date.fromisoformat(last_day_str)
    full_quarterly_stats = []
    while starting_date < last_day_of_data:
        if starting_date.month <= 3:
            ending_date = datetime.date.fromisoformat( f"{starting_date.year}-03-31")
        elif starting_date.month <= 6:
            ending_date = datetime.date.fromisoformat( f"{starting_date.year}-06-30")
        elif starting_date.month <= 9:
            ending_date = datetime.date.fromisoformat( f"{starting_date.year}-09-30")
        else:
            ending_date = datetime.date.fromisoformat( f"{starting_date.year}-12-31")
        
        print(f"\nAggregation date range: {starting_date.isoformat()} - {ending_date.isoformat()}")
        curr_date: str = starting_date.isoformat()

        quarterly_stats = {
            "date_range": (starting_date.isoformat(), ending_date.isoformat()),
            "drives": {},
        }
        while curr_date != ending_date.isoformat():
            curr_date = sorted_dates.pop(0)

            # Doing MAX aggregation, value is most of that drive we've seen in the quarter
            data_to_aggregate = daily_data[curr_date]
            for drive_model in data_to_aggregate:
                if drive_model not in quarterly_stats['drives']:
                    quarterly_stats['drives'][drive_model] = 0
            
                quarterly_stats['drives'][drive_model] = max(quarterly_stats['drives'][drive_model], data_to_aggregate[drive_model])

        full_quarterly_stats.append(quarterly_stats)

        starting_date = ending_date + datetime.timedelta(days=1)

    return full_quarterly_stats


def _main() -> None:
    args = _parse_args()
    drives_of_interest_regex = _read_drives_of_interest_regexes(args)
    #print(json.dumps(drives_of_interest_regex, indent=4))
    daily_drive_distribution: dict[str, dict[str, int]] = _get_daily_drive_distribution(args, drives_of_interest_regex)
    #print(json.dumps(daily_drive_distribution, indent=4, sort_keys=True))
    aggregated_data: list[dict[str, str | int]] = _aggregate_data(daily_drive_distribution)
    print(json.dumps(aggregated_data, indent=4, sort_keys=True)) 


if __name__ == "__main__":
    _main()

