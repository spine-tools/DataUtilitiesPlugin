from argparse import ArgumentParser
import sys
import xarray
from spinedb_api import (
    DatabaseMapping,
    import_alternatives,
    import_object_classes,
    import_object_parameter_values,
    import_object_parameters,
    import_objects,
    TimeSeriesVariableResolution,
)


def make_argument_parser():
    """Creates and configures an argument parser.

    Returns:
        ArgumentParser: a parser
    """
    parser = ArgumentParser(description="Imports time series from Atlite netCDF file.")
    parser.add_argument("object_class", help="name of target object class")
    parser.add_argument("parameter", help="name of target parameter")
    parser.add_argument("-a", default="Base", help="name of target alternative")
    parser.add_argument("file", nargs="+", help="input netCDF file(s)")
    parser.add_argument("url", help="target database URL")
    return parser


def valid(dataset):
    """Checks if given dataset is valid for importing.

    Args:
        dataset (Dataset): dataset to check

    Returns:
        bool: True if the dataset is valid, False otherwise.
    """
    n_dim = len(dataset.dims)
    if n_dim != 2:
        print(f"Dataset's dimensions should be 2, found {n_dim}.", file=sys.stderr)
        return False
    first_dim = next(iter(dataset.dims))
    if first_dim != "time":
        print(
            f"Expected dataset's first dimension to be 'time', found {first_dim}.",
            file=sys.stderr,
        )
        return False
    return True


def import_data_array(db_map, data_array, class_name, parameter, alternative):
    """Imports given data array into Spine database.

    Args:
        db_map (DatabaseMapping): a database map
        data_array (DataArray): data array to import
        class_name (str): object class name
        parameter (str): parameter name
        alternative (str): alternative name
    """
    entity_dim = data_array.dims[1]
    importable_objects = list()
    importable_values = list()
    for entity in data_array[entity_dim].values:
        time_stamps = data_array["time"].values
        values = data_array.loc[:, entity]
        time_series = TimeSeriesVariableResolution(time_stamps, values, False, False)
        importable_objects.append((class_name, entity))
        importable_values.append(
            (class_name, entity, parameter, time_series, alternative)
        )
    import_values(db_map, importable_objects, importable_values)


def import_dataset(db_map, dataset, class_name, parameter, alternative):
    """Imports given dataset into Spine database.

    Args:
        db_map (DatabaseMapping): a database map
        dataset (Dataset): dataset to import
        class_name (str): object class name
        parameter (str): parameter name
        alternative (str): alternative name
    """
    for data_array in dataset.values():
        import_data_array(db_map, data_array, class_name, parameter, alternative)


def import_object_class(db_map, class_name):
    """Imports an object class.

    Args:
        db_map (DatabaseMapping): a database map
        class_name (str): object class name
    """
    successes, errors = import_object_classes(db_map, (class_name,))
    if errors:
        print("Encountered errors while importing object classes:", file=sys.stderr)
        for error in errors:
            print(error, file=sys.stderr)
    if successes == 1:
        print(f"Imported object class '{class_name}'.")


def import_parameter(db_map, class_name, parameter):
    """Imports a parameter definition.

    Args:
        db_map (DatabaseMapping): a database map
        class_name (str): object class name
        parameter (str): parameter name
    """
    successes, errors = import_object_parameters(db_map, ((class_name, parameter),))
    if errors:
        print(
            "Encountered errors while importing object parameter definitions:",
            file=sys.stderr,
        )
        for error in errors:
            print(error, file=sys.stderr)
    if successes == 1:
        print(f"Imported parameter definition '{parameter}'.")


def import_alternative(db_map, alternative):
    """Imports an alternative.

    Args:
        db_map (DatabaseMapping): a database map
        alternative (str): alternative name
    """
    successes, errors = import_alternatives(db_map, (alternative,))
    if errors:
        print("Encountered errors while importing alternatives:", file=sys.stderr)
        for error in errors:
            print(error, file=sys.stderr)
    if successes == 1:
        print(f"Imported alternative '{alternative}'.")


def import_values(db_map, objects, values):
    """Imports time series.

    Args:
        db_map (DatabaseMapping): a database map
        objects (Iterable): object data to import
        values (Iterabe): object parameter value data to import
    """
    successes, errors = import_objects(db_map, objects)
    if errors:
        print("Encountered errors while importing objects:", file=sys.stderr)
        for error in errors:
            print(error, file=sys.stderr)
    print(f"Imported {successes} new objects.")
    successes, errors = import_object_parameter_values(db_map, values)
    if errors:
        print("Encountered errors while importing time series values:", file=sys.stderr)
        for error in errors:
            print(error, file=sys.stderr)
    print(f"Imported {successes} new time series.")


def process_files(file_names, url, class_name, parameter, alternative):
    """Opens given netCDF files and imports their contents to a Spine database.

    Args:
        file_names (Iterable of str): paths to input .nc files
        url (str): database URL
        class_name (str): object class name
        parameter (str): parameter name
        alternative (str): alternative name
    """
    db_map = DatabaseMapping(url)
    try:
        import_object_class(db_map, class_name)
        import_parameter(db_map, class_name, parameter)
        import_alternative(db_map, alternative)
        for file_name in file_names:
            print(f"Opening '{file_name}'")
            dataset = xarray.open_dataset(file_name)
            if not valid(dataset):
                print("Failed to open file.", file=sys.stderr)
                continue
            import_dataset(db_map, dataset, class_name, parameter, alternative)
        db_map.commit_session("Imported Atlite time series.")
    finally:
        db_map.connection.close()


if __name__ == "__main__":
    arg_parser = make_argument_parser()
    args = arg_parser.parse_args()
    process_files(args.file, args.url, args.object_class, args.parameter, args.a)
