#!/usr/bin/python3

import argparse
import csv
import datetime
import json
import re


def _parse_args() -> argparse.Namespace:
    arg_parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description='Compute annualized failure rate ("AFR")')
    arg_parser.add_argument('drive_stats_csv', help='CSV file with drive statistics')
    arg_parser.add_argument('computed_afr_csv', help='CSV file with computed daily AFR')

    args: argparse.Namespace = arg_parser.parse_args()

    return args


def _clean_drive_model_str(drive_model_str: str) -> str:
    """Remove leading/trailing whitespace, then convert one or more whitespace into single space.
    :param drive_model_str: Drive model string from CSV
    :return: model string with all extra whitespace removed
    """
    clean_str: str = re.sub(r'\s+', ' ', drive_model_str.strip())
    return clean_str


def _parse_csv_data(args: argparse.Namespace) -> dict[str, dict[str, dict[str, int]]]:

    parsed_data: dict[str, dict[str, dict[str, int]]] = {}
    with open(args.drive_stats_csv, 'r') as csv_handle:
        csv_reader = csv.reader(csv_handle)

        # Skip header row
        next(csv_reader)

        for curr_csv_row in csv_reader:
            drive_model: str = _clean_drive_model_str(curr_csv_row[0])
            #print(f"Drive model: {drive_model}")

            if drive_model not in parsed_data:
                parsed_data[drive_model]:dict[str, dict[str, int]] = {}

            curr_date: str = curr_csv_row[1]

            if curr_date not in parsed_data[drive_model]:
                parsed_data[drive_model][curr_date] = {
                    'drive_count': 0,
                    'failure_count': 0,
                }

            parsed_data[drive_model][curr_date]['drive_count'] += int(curr_csv_row[2])
            parsed_data[drive_model][curr_date]['failure_count'] += int(curr_csv_row[3])

    return parsed_data


def _prep_csv_data_for_afr_calc(parsed_csv_data: dict[str, dict[str, dict[str, int]]]
                                ) -> dict[str, list[dict[str, int | str]]]:
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


def _compute_afr_per_drive_model(
        afr_calc_prepped_data: dict[str, list[dict[str, str | int | float]]]):

    computed_afr_data: dict[str, list[dict[str, str | int | float]]] = {}

    for curr_drive_model_name in afr_calc_prepped_data:
        computed_afr_data[ curr_drive_model_name ]: list[dict[str, str | int | float]] = []

        for curr_datapoint in afr_calc_prepped_data[ curr_drive_model_name ]:

            # Scaling factor is 365 unit-days / year
            afr_scaling_factor = 365.0

            annualized_failure_rate = (curr_datapoint[ 'cumulative_drive_failures' ] /
                                       curr_datapoint[ 'cumulative_drive_days' ]) * afr_scaling_factor

            computed_afr_data[ curr_drive_model_name ].append(
                {
                    "afr_percent"   : round(annualized_failure_rate * 100.0, 2),
                    "date"          : curr_datapoint['date'],
                    "day_index"     : curr_datapoint['day_index'],
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
            'drive_model',
            'day_index',
            'date',
            'annualized_failure_rate_percent',
        )
        csv_writer.writerow(field_names)

        # Deal with data rows
        for curr_drive_model in sorted_drive_model_families:
            for curr_afr_datapoint in drive_model_family_afr_data[curr_drive_model]:
                #print(json.dumps(curr_afr_datapoint))
                current_row: tuple[str | int | float, ...] = (
                        curr_drive_model,
                        curr_afr_datapoint[ 'day_index' ],
                        curr_afr_datapoint[ 'date' ],
                        f"{curr_afr_datapoint[ 'afr_percent' ]:.02f}",
                    )

                csv_writer.writerow( current_row)


def _main() -> None:
    args: argparse.Namespace = _parse_args()
    parsed_csv_data: dict[str, dict[str, dict[str, int]]] = _parse_csv_data(args)
    #print( json.dumps(parsed_csv_data, indent=4, sort_keys=True) )

    afr_prepped_data: dict[str, list[dict[str, int | str]]] = _prep_csv_data_for_afr_calc( parsed_csv_data )

    drive_model_afr_data: dict[str, list[dict[str, int | str]]] = \
        _compute_afr_per_drive_model(afr_prepped_data)
    _write_afr_data_to_csv( args, drive_model_afr_data )


if __name__ == "__main__":
    _main()
