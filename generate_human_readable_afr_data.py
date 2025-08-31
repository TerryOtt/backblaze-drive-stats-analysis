import argparse
import csv
import datetime
import json
import math
import re


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert AFR CSV to human-readable CSV")
    parser.add_argument("trino_csv", help="Path to CSV with raw data from Trino")
    parser.add_argument("drive_model_regex_json", help="Path to JSON file with drive model regexes")
    parser.add_argument("human_readable_csv", help="Path to human-readable CSV")

    return parser.parse_args()


def _read_trino_csv(args: argparse.Namespace) -> dict[str, dict[str, dict[str, int]]]:
    known_models: set[str] = set()
    trino_data_by_drive_model: dict[str, dict[str, dict[str, int]]] = {}
    with open(args.trino_csv, "r") as afr_csv:
        csv_reader:csv.DictReader[str] = csv.DictReader(afr_csv)

        for curr_csv_row in csv_reader:
            cleaned_model: str = _clean_drive_model_str(curr_csv_row['model'])
            # Is this a new drive model?
            if cleaned_model not in known_models:
                #print(f"Found new model {cleaned_model}")
                known_models.add(cleaned_model)
                model_day_index = 1
                trino_data_by_drive_model[cleaned_model] = {}

            row_date: str = datetime.date.fromisoformat(curr_csv_row['date']).isoformat()

            # Is this a new date for this drive?
            if row_date not in trino_data_by_drive_model[cleaned_model]:
                trino_data_by_drive_model[cleaned_model][row_date] = {
                    'drive_count'   : 0,
                    'failure_count' : 0,
                }

            trino_data_by_drive_model[cleaned_model][row_date]['drive_count'] += \
                int(curr_csv_row['drive_count'])
            trino_data_by_drive_model[cleaned_model][row_date]['failure_count'] += \
                int(curr_csv_row['failure_count'])

    return trino_data_by_drive_model


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
        # 	If the regular expression matches, use final token as drive model
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


def _generate_human_readable_data(
        args: argparse.Namespace,
        trino_data: dict[str, dict[str, dict[str, int]]]
) -> dict[str, list[dict[str, int]]]:
    human_readable_data: dict[str, list[dict[str, int]]] = {}

    with open(args.drive_model_regex_json, "r") as regex_json_handle:
        json_regexes: list[str] = json.load(regex_json_handle)

    min_max_deployed_drives: int = 1000

    days_in_data_aggregation_increment: int = 91
    agg_increments_per_year: int = 4

    for curr_drive_model in trino_data:
        curr_aggregation_increment: int = 1
        drive_model_we_care_about: bool = False
        for curr_drive_model_regex in json_regexes:
            if re.search(curr_drive_model_regex, curr_drive_model):
                drive_model_we_care_about = True
                break

        if not drive_model_we_care_about:
            continue

        max_drives_deployed: int = _get_max_drives_deployed( curr_drive_model, trino_data[curr_drive_model] )
        if max_drives_deployed < min_max_deployed_drives:
            continue

        print(f"Drive model of interest: {curr_drive_model:15s} ({max_drives_deployed:7,} drives)")

        # Walk a quarter at a time, unless we don't have that many dates left
        agg_year: int = 0
        while trino_data[curr_drive_model]:
            sorted_drive_model_dates: list[str] = sorted( trino_data[curr_drive_model])
            days_to_walk: int = min(days_in_data_aggregation_increment, len(sorted_drive_model_dates))

            start_date: str = sorted_drive_model_dates[0]
            end_date: str = sorted_drive_model_dates[days_to_walk - 1]

            agg_quarter: int = curr_aggregation_increment % agg_increments_per_year

            if agg_quarter == 0:
                agg_quarter = 4
            elif agg_quarter == 1:
                agg_year += 1

            print(f"\tYear {agg_year}, Quarter {agg_quarter} ({days_to_walk:2d} days, {start_date} - {end_date})")

            for curr_date in sorted_drive_model_dates[:days_to_walk]:
                #print(f"\t\tCurr date: {curr_date}")

                # Get copy of data at this date
                today_data: dict[str, int] = trino_data[curr_drive_model][curr_date]

                #print(json.dumps(today_data, indent=2, sort_keys=True))

                # Delete this dates's data from parent data
                del trino_data[curr_drive_model][curr_date]

            curr_aggregation_increment += 1



        # Walk data a quarter at a time

        max_afr_in_quarter: float = 0.0

    return human_readable_data


def _get_max_drives_deployed(curr_drive_model: str, trino_data_drive: dict[str, dict[str, int]]) -> int:
    max_drives_deployed: int = 0

    # Walk all dates
    for curr_date in trino_data_drive:
        drives_deployed: int = trino_data_drive[curr_date]['drive_count']
        if drives_deployed > max_drives_deployed:
            max_drives_deployed = drives_deployed

    return max_drives_deployed


def _main():
    args: argparse.Namespace = _parse_args()

    trino_data: dict[str, dict[str, dict[str, int]]] = _read_trino_csv(args)
    _generate_human_readable_data(args, trino_data)

    #print(json.dumps(trino_data, indent=4, sort_keys=True))


if __name__ == "__main__":
    _main()
