import argparse
import csv
import datetime


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert raw Trino CSV to Excel CSV")
    parser.add_argument("trino_csv", help="Path to Trino CSV")
    parser.add_argument("excel_csv", help="Path to Excel CSV")

    return parser.parse_args()


def _convert_afr_csv_to_excel(
        args: argparse.Namespace
 ) -> tuple[list[str], dict[str, dict[str, int]], dict[int, dict[str, float]]]:

    drive_model_column_names: set[str] = set()
    excel_data: dict[int, dict[str, float]] = {}
    drive_model_first_last_seen: dict[str, dict[str, int]] = {}
    with open(args.trino_csv, "r") as trino_csv:
        csv_reader:csv.DictReader[str] = csv.DictReader(trino_csv)

        for curr_csv_line in csv_reader:
            day_index: int = int(curr_csv_line['day_index'])
            drive_model: str = curr_csv_line['drive_model']

            # Is this the first time we've seen this column name?
            if drive_model not in drive_model_column_names:
                drive_model_column_names.add(drive_model)
                drive_model_first_last_seen[drive_model] = {
                    'first_seen'    : day_index,
                    'last_seen'     : day_index,
                }

            # Do we need to bump up last seen?
            if day_index > drive_model_first_last_seen[drive_model]['last_seen']:
                drive_model_first_last_seen[drive_model]['last_seen'] = day_index

            # Does day index exist in excel data?
            if day_index not in excel_data:
                excel_data[day_index]: dict[str, float] = {}

            # Append AFR for this day
            excel_data[day_index][drive_model] = float(curr_csv_line['annualized_failure_rate_percent'])

    sorted_column_names: list[str] = sorted(drive_model_column_names)

    return sorted_column_names, drive_model_first_last_seen, excel_data


def _create_excel_csv(
        args: argparse.Namespace,
        drive_model_column_names: list[str],
        drive_model_first_last_seen: dict[str, dict[str, int]],
        excel_data: dict[int, dict[str, float]] ) -> None:

    csv_columns: list[str] = [
        'day_index',
    ]

    # Populate rest of column names
    csv_columns.extend(drive_model_column_names)

    # for curr_column in csv_columns[1:]:
    #     print(f"Drive model: {curr_column}")

    with open(args.excel_csv, "w") as excel_csv_handle:
        csv_writer: csv.DictWriter = csv.DictWriter(excel_csv_handle, fieldnames=csv_columns)

        # Write header row
        csv_writer.writeheader()

        for day_index in sorted(excel_data):

            data_row: dict[str, int | str] = {
                'day_index': day_index,
            }

            for curr_drive_model in drive_model_column_names:
                # Does Excel data have AFR data for this drive on the given day?
                if curr_drive_model in excel_data[day_index]:
                    data_row[curr_drive_model] = f"{excel_data[day_index][curr_drive_model]:.02f}"

                # If this is just a missing datapoint, copy most recent AFR datapoint  through to avoid
                #       series line to zero
                elif (drive_model_first_last_seen[curr_drive_model]['first_seen']
                      < day_index
                      < drive_model_first_last_seen[curr_drive_model]['last_seen']):

                    search_day_index:int = day_index - 1
                    while True:
                        if curr_drive_model in excel_data[search_day_index]:
                            data_row[curr_drive_model] = f"{excel_data[search_day_index][curr_drive_model]:.02f}"
                            break
                        search_day_index -= 1
                        assert search_day_index >= 1

            csv_writer.writerow(data_row)

            # Write one data row and bail
            break


def _main():

    args: argparse.Namespace = _parse_args()

    returned_tuple: tuple[list[str], dict[str, dict[str, int]], dict[int, dict[str, float]]] = \
        _convert_afr_csv_to_excel(args)

    drive_model_column_names: list[str] = returned_tuple[0]
    drive_model_first_last_seen: dict[str, dict[str, int]] = returned_tuple[1]
    excel_data: dict[int, dict[str, float]] = returned_tuple[2]

    _create_excel_csv(args, drive_model_column_names, drive_model_first_last_seen, excel_data)


if __name__ == "__main__":
    _main()
