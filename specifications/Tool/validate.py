from argparse import ArgumentParser
import json
import re
import sys
from cerberus import Validator, TypeDefinition
from spinedb_api import (
    Array,
    DatabaseMapping,
    DateTime,
    Duration,
    from_database,
    Map,
    TimeSeries,
    TimePattern,
)
from spinedb_api.parameter_value import map_dimensions


class SpineValidator(Validator):
    """A validator with additional Spine data specific types and validation rules."""

    types_mapping = Validator.types_mapping.copy()
    types_mapping["array"] = TypeDefinition("array", (Array,), ())
    # Note: the following replaces build-in 'datetime' type
    types_mapping["datetime"] = TypeDefinition("datetime", (DateTime,), ())
    types_mapping["duration"] = TypeDefinition("duration", (Duration,), ())
    types_mapping["map"] = TypeDefinition("map", (Map,), ())
    types_mapping["time series"] = TypeDefinition("time series", (TimeSeries,), ())
    types_mapping["time pattern"] = TypeDefinition("time pattern", (TimePattern,), ())

    def _validate_min_indexes(self, constraint, field, value):
        """Tests minimum number of indexes of a value.

        The rule's arguments are validated against this schema:
        {'type': 'integer', 'min': 0}
        """
        if self._count_indexes(value) < constraint:
            self._error(field, f"must have index count > {constraint}")

    def _validate_max_indexes(self, constraint, field, value):
        """Tests maximum number of indexes of a value.

        The rule's arguments are validated against this schema:
        {'type': 'integer', 'min': 0}
        """
        if self._count_indexes(value) > constraint:
            self._error(field, f"must have index count < {constraint}")

    def _validate_number_of_indexes(self, constraint, field, value):
        """Tests exact number of indexes of a value.

        The rule's arguments are validated against this schema:
        {'type': 'integer', 'min': 0}
        """
        if self._count_indexes(value) != constraint:
            self._error(field, f"must have index count of {constraint}")

    @staticmethod
    def _count_indexes(value):
        """Counts the number of indexes in given value.

        Args:
            value (Any): value to inspect

        Returns:
            int: number of indexes
        """
        if isinstance(value, (Array, TimePattern, TimeSeries)):
            return 1
        if isinstance(value, Map):
            return map_dimensions(value)
        return 0


class ValidationFailed(Exception):
    """Exception to signal that validation failed."""


def make_argument_parser():
    """Creates and configures an argument parser.

    Returns:
        ArgumentParser: a parser
    """
    parser = ArgumentParser(description="Validates Spine databases.")
    parser.add_argument("schema", help="path to validation schema file")
    parser.add_argument("url", nargs="+", help="one or more database urls")
    return parser


def build_message_for_object_row(db_row, errors):
    """Constructs a message after validation of a rule has failed.
    
    Args:
        db_row (namedtuple): database row for which validation failed
        errors (dict): Cerberus errors

    Returns:
        str: a message
    """
    address = " - ".join(
        (
            db_row.object_class_name,
            db_row.parameter_name,
            db_row.object_name,
            db_row.alternative_name,
        )
    )
    return f"{address}: {'; '.join(errors['value'])}"


def build_message_for_relationship_row(db_row, errors):
    """Constructs a message after validation of a rule has failed.

    Args:
        db_row (namedtuple): database row for which validation failed
        errors (dict): Cerberus errors

    Returns:
        str: a message
    """
    address = " - ".join(
        (
            db_row.relationship_class_name,
            db_row.parameter_name,
            db_row.object_name_list,
            db_row.alternative_name,
        )
    )
    return f"{address}: {'; '.join(errors['value'])}"


class ObjectParameterValuePattern:
    """A pattern that can be compared against object parameter value database rows."""

    def __init__(self, class_re, parameter_re, object_re, alternative_re):
        """
        Args:
            class_re (str): regular expression to match object class names
            parameter_re (str): regular expression to match parameter names
            object_re (str): regular expression to match object names
            alternative_re (str): regular expression to match alternative names
        """
        self._class = re.compile(class_re)
        self._parameter = re.compile(parameter_re)
        self._object = re.compile(object_re)
        self._alternative = re.compile(alternative_re)

    def matches(self, db_row):
        """Matches database row to the pattern.

        Args:
            db_row (namedtuple): an object parameter value database row

        Returns:
            bool: True if the row matches, False otherwise
        """
        if self._class.match(db_row.object_class_name) is None:
            return False
        if self._parameter.match(db_row.parameter_name) is None:
            return False
        if self._object.match(db_row.object_name) is None:
            return False
        if self._alternative.match(db_row.alternative_name) is None:
            return False
        return True


class RelationshipParameterValuePattern:
    """A pattern that can be compared against relationship parameter value database rows."""

    def __init__(self, class_re, parameter_re, object_res, alternative_re):
        """
        Args:
            class_re (str): regular expression to match object class names
            parameter_re (str): regular expression to match parameter names
            object_res (Iterable of str): regular expressions to match names
            alternative_re (str): regular expression to match alternative names
        """
        self._class = re.compile(class_re)
        self._parameter = re.compile(parameter_re)
        self._objects = list(map(re.compile, object_res))
        self._alternative = re.compile(alternative_re)

    def matches(self, db_row):
        if self._class.match(db_row.relationship_class_name) is None:
            return False
        if self._parameter.match(db_row.parameter_name) is None:
            return False
        objects = db_row.object_name_list.split(",")
        if len(objects) != len(self._objects) or any(
            pattern.match(name) is None for pattern, name in zip(self._objects, objects)
        ):
            return False
        if self._alternative.match(db_row.alternative_name) is None:
            return False
        return True


def validate(db_map, settings):
    """Validates a database.

    Args:
        db_map (DatabaseMappingBase): database mappings
        settings (dict): validation settings

    Returns:
        list of str: list of error messages if validation fails
    """
    object_parameter_rules = settings.get("object_parameter_value", [])
    relationship_parameter_rules = settings.get("relationship_parameter_value", [])
    errors = list()
    for rule in object_parameter_rules:
        value_pattern = ObjectParameterValuePattern(
            rule.get("class", ""),
            rule.get("parameter", ""),
            rule.get("object", ""),
            rule.get("alternative", ""),
        )
        validator = SpineValidator({"value": rule["rule"]})
        for db_row in db_map.query(db_map.object_parameter_value_sq):
            if not value_pattern.matches(db_row):
                continue
            valid = validator.validate({"value": from_database(db_row.value)})
            if not valid:
                message = build_message_for_object_row(db_row, validator.errors)
                errors.append(message)
    for rule in relationship_parameter_rules:
        value_pattern = RelationshipParameterValuePattern(
            rule.get("class", ""),
            rule.get("parameter", ""),
            rule["objects"],
            rule.get("alternative", ""),
        )
        validator = SpineValidator({"value": rule["rule"]})
        for db_row in db_map.query(db_map.relationship_parameter_value_sq):
            if not value_pattern.matches(db_row):
                continue
            valid = validator.validate({"value": from_database(db_row.value)})
            if not valid:
                message = build_message_for_relationship_row(db_row, validator.errors)
                errors.append(message)
    return errors


def validate_urls(urls, settings):
    """Validates databases.

    Args:
        url (Iterable of str): database URLs
        settings (dict): validation settings

    Returns:
        dict: mapping from URL to list of validation errors
    """
    errors = dict()
    for url in urls:
        db_map = DatabaseMapping(url)
        try:
            failures = validate(db_map, settings)
            if failures:
                errors[url] = failures
        finally:
            db_map.connection.close()
    return errors


def print_errors(errors, out_stream):
    """Prints errors to given stream.

    Args:
        errors (dict): mapping from database URL to list of error messages
        out_stream (IOBase): a file like object for output
    """
    for url, messages in errors.items():
        print(f"in {url}:", file=out_stream)
        for error in messages:
            print("    " + error, file=out_stream)


if __name__ == "__main__":
    arg_parser = make_argument_parser()
    args = arg_parser.parse_args()
    with open(args.schema) as settings_file:
        validation_settings = json.load(settings_file)
    errors = validate_urls(args.url, validation_settings)
    if errors:
        print_errors(errors, sys.stderr)
        raise ValidationFailed()
