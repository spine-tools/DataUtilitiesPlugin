import sys
from argparse import ArgumentParser
from enum import Enum, unique
import numpy as np
from scipy.interpolate import interp1d
from spinedb_api import (
    DatabaseMapping,
    from_database,
    TimeSeriesFixedResolution,
    TimeSeriesVariableResolution,
    to_database,
)


@unique
class Interpolation(Enum):
    """Available interpolation types."""

    PREVIOUS = "previous"
    NEXT = "next"
    NEAREST = "nearest"
    LINEAR = "linear"


interpolation_choices = [i.value for i in Interpolation]


def _make_arg_parser():
    """Initializes an argument parser.

    Returns:
        ArgumentParser: parser
    """
    parser = ArgumentParser()
    parser.add_argument(
        "interpolation", choices=interpolation_choices, help="interpolation method"
    )
    parser.add_argument("url", nargs="+", help="database URL")
    return parser


def fill(time_series, interpolation_method):
    """Interpolates missing values in time series.

    Args:
        time_series (TimeSeriesVariableResolution): time series to fill
        interpolation_method (Interpolation): interpolation method

    Returns:
        TimeSeriesFixedResolution: filled time series
    """
    diffs = np.diff(time_series.indexes)
    step = np.amin(diffs)
    fractional_steps = diffs / step
    steps = np.rint(fractional_steps)
    if float(np.amax(np.abs(steps - fractional_steps))) > 1e-8:
        return None
    filled_values = list()
    for i, step_count in enumerate(steps):
        filled_values.append(time_series.values[i])
        if step_count > 1.0:
            interpolation = interp1d(
                time_series.indexes[i : i + 2].astype("float64"),
                time_series.values[i : i + 2],
                kind=interpolation_method.value,
            )
            interpolation_stamps = np.asarray(
                [time_series.indexes[i] + n * step for n in np.arange(1.0, step_count)]
            )
            filled_values += list(interpolation(interpolation_stamps.astype("float64")))
    filled_values.append(time_series.values[-1])
    new_time_series = TimeSeriesFixedResolution(
        time_series.indexes[0],
        str(step),
        filled_values,
        time_series.ignore_year,
        time_series.repeat,
        time_series.index_name,
    )
    return new_time_series


def process_database(db_map, interpolation_method):
    """Processes all time series in a database.

    Args:
        db_map (DatabaseMapping): database map
        interpolation_method (Interpolation): interpolation method

    Returns:
        list of dict: database update data
    """
    updated_values = list()
    for value_row in db_map.query(db_map.parameter_value_sq):
        if value_row.type != "time_series":
            continue
        time_series = from_database(value_row.value, value_row.type)
        if (
            not isinstance(time_series, TimeSeriesVariableResolution)
            or len(time_series) < 3
        ):
            continue
        new_time_series = fill(time_series, interpolation_method)
        if new_time_series is None:
            print(f"Couldn't fill a time series.", file=sys.stderr)
            continue
        new_value, type_ = to_database(new_time_series)
        updated_values.append({"id": value_row.id, "value": new_value, "type": type_})
    return updated_values


def process_all(urls, interpolation_method):
    """Processes all databases.

    Args:
        urls (Iterable of str): database URLs
        interpolation_method (Interpolation): interpolation method
    """
    for url in urls:
        db_map = DatabaseMapping(url)
        try:
            db_map.update_parameter_values(
                *process_database(db_map, interpolation_method)
            )
            db_map.commit_session("Interpolated missing points in time series.")
        finally:
            db_map.connection.close()


def main(parser):
    """Executes the script.

    Args:
        parser (ArgumentParser): command line argument parser

    Returns:
        int: exit status
    """
    args = parser.parse_args()
    interpolation_method = Interpolation(args.interpolation)
    process_all(args.url, interpolation_method)
    return 0


if __name__ == "__main__":
    arg_parser = _make_arg_parser()
    arg_parser.exit(status=main(arg_parser))
