#!/usr/bin/python3

import argparse
import csv
import datetime
import json
import re


def _parse_args() -> argparse.Namespace:
    arg_parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description='Compute annualized failure rate ("AFR")')
    arg_parser.add_argument('drive_model_families_json',
                            help='JSON file with drive model family mappings')
    arg_parser.add_argument('drive_stats_csv', help='CSV file with drive statistics')
    arg_parser.add_argument('computed_afr_csv', help='CSV file with computed daily AFR')

    args: argparse.Namespace = arg_parser.parse_args()

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
    """Remove leading/trailing whitespace, then convert one or more whitespace into single space.
    :param drive_model_str: Drive model string from CSV
    :return: model string with all extra whitespace removed
    """
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
                        # print(f"{drive_model} -> {current_mapping['drive_model_family']} "
                        #       f"(regex: {current_mapping['regex']})")

                        current_model_family = current_mapping['drive_model_family']

                        if current_model_family not in parsed_data:
                            parsed_data[current_model_family] = {}
                            # print(f"Added {current_model_family} to parsed data")

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


def _prep_csv_data_for_afr_calc(
        parsed_csv_data: dict[str, dict[str, dict[str, int]]] ) -> dict[str, list[dict[str, int | str]]]:
    """Take CSV data and move it to a list of dicts, with day index and cumulative stats. """

    #print("Prepping data")

    afr_prepped_data: dict[str, list[dict[str, int | str]]] = {}

    for drive_model_family in parsed_csv_data:
        # Get sorted list of dates
        sorted_dates = sorted([datetime.date.fromisoformat(datestr)
                               for datestr in parsed_csv_data[drive_model_family]])

        afr_prepped_data[drive_model_family]: list[dict[str, int | str]] = []

        start_date: datetime.date = sorted_dates[0]
        cumulative_data: dict[str, int] = {
            'drive_days'    : 0,
            'failures'      : 0,
        }

        for curr_date in sorted_dates:
            # Initialize prepped data from CSV data and then enrich it
            prepped_data: dict[str, int | str] = parsed_csv_data[drive_model_family][curr_date.isoformat()]

            cumulative_data['drive_days']   += prepped_data['drive_count']
            cumulative_data['failures']     += prepped_data['failure_count']

            prepped_data.update(
                {
                    'date'                      : curr_date.isoformat(),
                    'day_index'                 : (curr_date - start_date).days + 1,
                    'cumulative_drive_days'     : cumulative_data['drive_days'],
                    'cumulative_drive_failures' : cumulative_data['failures'],
                }
            )
            afr_prepped_data[drive_model_family].append(prepped_data)

    #print(json.dumps(afr_prepped_data, indent=4, sort_keys=True, default=str))

    return afr_prepped_data


def _compute_afr_per_drive_model_family(
        afr_calc_prepped_data: dict[str, list[dict[str, str | int | float]]]):

    computed_afr_data: dict[str, list[dict[str, str | int | float]]] = {}

    for curr_model_family_name in afr_calc_prepped_data:
        computed_afr_data[ curr_model_family_name ] = []

        for curr_datapoint in afr_calc_prepped_data[curr_model_family_name]:

            # Scaling factor is 365 unit-days / year
            afr_scaling_factor = 365.0

            annualized_failure_rate = (curr_datapoint[ 'cumulative_drive_failures' ] /
                                       curr_datapoint[ 'cumulative_drive_days' ]) * afr_scaling_factor

            computed_afr_data[ curr_model_family_name ].append(
                {
                    "day_index"     : curr_datapoint['day_index'],
                    "afr_percent"   : round(annualized_failure_rate * 100.0, 2),
                    #"cumulative_drive_failures" : curr_datapoint['cumulative_drive_failures'],
                    #"cumulative_drive_days"     : curr_datapoint['cumulative_drive_days'],
                }
            )

    return computed_afr_data


def _write_afr_data_to_csv(args: argparse.Namespace,
                           drive_model_family_afr_data: dict[str, list[dict[str, str | int ]]]) -> None:

    sorted_drive_model_families: list[str] = sorted(drive_model_family_afr_data.keys())

    with open(args.computed_afr_csv, 'w') as file_handle:
        csv_writer = csv.writer(file_handle)

        # Write header row
        field_names: tuple[str, ...] = (
            'drive_model_family',
            'day_index',
            'annualized_failure_rate_percent',
        )
        csv_writer.writerow(field_names)

        # Deal with data rows
        for curr_drive_model_family in sorted_drive_model_families:
            for curr_afr_datapoint in drive_model_family_afr_data[curr_drive_model_family]:
                #print(json.dumps(curr_afr_datapoint))
                current_row: tuple[str | int | float, ...] = (
                        curr_drive_model_family,
                        curr_afr_datapoint[ 'day_index' ],
                        f"{curr_afr_datapoint[ 'afr_percent' ]:.02f}",
                    )

                csv_writer.writerow( current_row)


def _main() -> None:
    args: argparse.Namespace = _parse_args()
    drive_model_family_mappings: list[dict[str, str]] = _generate_regex_map(args)
    parsed_csv_data: dict[str, dict[str, dict[str, int]]] = _parse_csv_data(
        args, drive_model_family_mappings)
    #print( json.dumps(parsed_csv_data, indent=4, sort_keys=True) )

    afr_prepped_data: dict[str, list[dict[str, int | str]]] = _prep_csv_data_for_afr_calc( parsed_csv_data )

    drive_model_family_afr_data: dict[str, list[dict[str, int | str]]] = \
        _compute_afr_per_drive_model_family(afr_prepped_data)
    _write_afr_data_to_csv( args, drive_model_family_afr_data )


if __name__ == "__main__":
    _main()
