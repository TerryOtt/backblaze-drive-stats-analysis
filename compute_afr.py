#!/usr/bin/python3

import argparse
import csv
import datetime
import json
import re


def _parse_args() -> argparse.Namespace:
    argparser: argparse.ArgumentParser = argparse.ArgumentParser(
        description='Compute annualized failure rate ("AFR")')
    argparser.add_argument('drive_model_families_json',
                           help='JSON file with drive model family mappings')
    argparser.add_argument('drive_stats_csv',
                           help='CSV file with drive statistics')

    args: argparse.Namespace = argparser.parse_args()

    return args


def _generate_regex_map(args: argparse.Namespace) -> list[dict[str, str]]:
    with open(args.drive_model_families_json, 'r') as json_handle:
        json_dict: dict[str, str] = json.load(json_handle)

    drive_model_mappings: list[dict[str, str]] = []

    for drive_model_family in json_dict:
        for drive_model_regex in json_dict[drive_model_family]:
            drive_model_mappings.append(
                {
                    'regex'                 : drive_model_regex,
                    'drive_model_family'    : drive_model_family,
                }
            )

    return drive_model_mappings


def _clean_drive_model_str(drive_model_str: str) -> str:
    # Remove leading/trailing whitespace, then convert one or more whitespace into single space
    clean_str: str = re.sub(r'\s+', ' ', drive_model_str.strip())
    return clean_str


def _parse_csv_data(args: argparse.Namespace,
                    drive_model_family_mappings: list[dict[str, str]]
                    ) -> dict[str, dict[str, dict[str, int]]]:

    parsed_data: dict[str, dict[str, dict[str, int]]] = {}
    with open(args.drive_stats_csv, 'r') as csv_handle:
        csv_reader = csv.reader(csv_handle)

        # Skip header row
        next(csv_reader)

        # Read data rows
        current_model:str | None = None
        current_model_family:str | None = None

        for curr_csv_row in csv_reader:
            drive_model: str = _clean_drive_model_str(curr_csv_row[0])
            #print(f"Drive model: {drive_model}")
            curr_date: str = curr_csv_row[1]

            # Do we need to need to deal with a model change
            if drive_model != current_model:
                # Reset model family, it's a new drive model
                current_model = drive_model
                current_model_family = None

                # Walk it over all drive model -> drive model family regexes to see if we got a match
                for current_mapping in drive_model_family_mappings:
                    #print(f"\tTrying match: {current_mapping['regex']}")
                    if re.search(current_mapping['regex'], drive_model):
                        print(f"{drive_model} -> {current_mapping['drive_model_family']} "
                              f"(regex: {current_mapping['regex']})")

                        current_model_family = current_mapping['drive_model_family']

                        if current_model_family not in parsed_data:
                            parsed_data[current_model_family] = {}
                            print(f"Added {current_model_family} to parsed data")

            # If we don't have a drive model family for this drive by the time we get here,
            #       we don't care about the drive model
            if current_model_family is None:
                # Skip this drive
                continue

            if curr_date not in parsed_data[current_model_family]:
                parsed_data[current_model_family][curr_date] = {
                    'drive_count': 0,
                    'failure_count': 0,
                }

            parsed_data[current_model_family][curr_date]['drive_count'] += int(curr_csv_row[2])
            parsed_data[current_model_family][curr_date]['failure_count'] += int(curr_csv_row[3])

    return parsed_data

def _main() -> None:
    args: argparse.Namespace = _parse_args()
    drive_model_family_mappings: list[dict[str, str]] = _generate_regex_map(args)
    parsed_csv_data:dict[str, dict[str, dict[str, int]]] = _parse_csv_data(args, drive_model_family_mappings)
    print( json.dumps(parsed_csv_data, indent=4, sort_keys=True) )


if __name__ == "__main__":
    _main()