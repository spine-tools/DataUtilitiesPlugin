import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from spinedb_api import (
    DatabaseMapping,
    import_object_classes,
    import_object_parameters,
    import_object_parameter_values,
    import_objects,
    import_relationship_classes,
    import_relationship_parameters,
    import_relationship_parameter_values,
    import_relationships,
)
from specifications.Tool.validate import validate


class TestValidateFunction(unittest.TestCase):
    def setUp(self):
        self._db_map = DatabaseMapping("sqlite:///", create=True)

    def tearDown(self):
        self._db_map.connection.close()

    def _add_object_parameter(self, value):
        result = import_object_classes(self._db_map, ("o_class",))
        self.assertEqual(result, (1, []))
        result = import_object_parameters(self._db_map, (("o_class", "param"),))
        self.assertEqual(result, (1, []))
        result = import_objects(self._db_map, (("o_class", "obj"),))
        self.assertEqual(result, (1, []))
        result = import_object_parameter_values(
            self._db_map, (("o_class", "obj", "param", value),)
        )
        self.assertEqual(result, (1, []))

    def _add_2D_relationship_and_parameter(self, value):
        result = import_object_classes(self._db_map, ("o_class", "p_class"))
        self.assertEqual(result, (2, []))
        result = import_objects(
            self._db_map, (("o_class", "o_obj"), ("p_class", "p_obj"))
        )
        self.assertEqual(result, (2, []))
        result = import_relationship_classes(
            self._db_map, (("r_ship", ("o_class", "p_class")),)
        )
        self.assertEqual(result, (1, []))
        result = import_relationship_parameters(self._db_map, (("r_ship", "param"),))
        self.assertEqual(result, (1, []))
        result = import_relationships(self._db_map, (("r_ship", ("o_obj", "p_obj")),))
        self.assertEqual(result, (1, []))
        result = import_relationship_parameter_values(
            self._db_map, (("r_ship", ("o_obj", "p_obj"), "param", value),)
        )
        self.assertEqual(result, (1, []))

    def test_empty_database_and_empty_settings(self):
        errors = validate(self._db_map, [], [])
        self.assertEqual(errors, [])

    def test_single_object_value_passes(self):
        self._add_object_parameter(23.0)
        rules = [
            {
                "class": "o_class",
                "parameter": "param",
                "object": "obj",
                "alternative": "Base",
                "rule": {"type": "number", "min": 0.0, "max": 50.0},
            }
        ]
        errors = validate(self._db_map, rules, [])
        self.assertEqual(errors, [])

    def test_single_object_value_fails_max_check(self):
        self._add_object_parameter(23.0)
        rules = [
            {
                "class": "o_class",
                "parameter": "param",
                "object": "obj",
                "alternative": "Base",
                "rule": {"max": 0.0},
            }
        ]
        errors = validate(self._db_map, rules, [])
        self.assertEqual(errors, ["o_class - param - obj - Base: max value is 0.0"])

    def test_single_relationship_value_passes(self):
        self._add_2D_relationship_and_parameter(23.0)
        rules = [
            {
                "class": "r_ship",
                "parameter": "param",
                "objects": ["o_obj", "p_obj"],
                "alternative": "Base",
                "rule": {"type": "number", "min": 0.0, "max": 50.0},
            }
        ]
        errors = validate(self._db_map, [], rules)
        self.assertEqual(errors, [])

    def test_single_relationship_value_fails_type_check(self):
        self._add_2D_relationship_and_parameter(23.0)
        rules = [
            {
                "class": "r_ship",
                "parameter": "param",
                "objects": ["o_obj", "p_obj"],
                "alternative": "Base",
                "rule": {"type": "integer"},
            }
        ]
        errors = validate(self._db_map, [], rules)
        self.assertEqual(
            errors, ["r_ship - param - o_obj,p_obj - Base: must be of integer type"]
        )

    def test_additional_type_rules_fail_correctly(self):
        self._add_object_parameter(23.0)
        for type_ in (
            "array",
            "datetime",
            "duration",
            "map",
            "time pattern",
            "time series",
        ):
            rules = [{"rule": {"type": type_}}]
            errors = validate(self._db_map, rules, [])
            self.assertEqual(
                errors, [f"o_class - param - obj - Base: must be of {type_} type"]
            )

    def test_number_of_indexes_rule(self):
        self._add_object_parameter(23.0)
        for type_ in (
            "array",
            "datetime",
            "duration",
            "map",
            "time pattern",
            "time series",
        ):
            rules = [{"rule": {"number of indexes": 2}}]
            errors = validate(self._db_map, rules, [])
            self.assertEqual(
                errors, [f"o_class - param - obj - Base: must have index count of 2"]
            )


class TestScript(unittest.TestCase):
    _script_path = (
        Path(__file__).parent.parent / "specifications" / "Tool" / "validate.py"
    )

    def setUp(self):
        self._temp_dir = TemporaryDirectory()
        self._url = "sqlite:///" + str(Path(self._temp_dir.name, "test_db.sqlite"))
        self._db_map = DatabaseMapping(self._url, create=True)

    def tearDown(self):
        self._temp_dir.cleanup()

    def test_empty_database_and_empty_settings(self):
        self._db_map.connection.close()
        settings_path = Path(self._temp_dir.name, "validate_settings.json")
        with open(settings_path, "w") as settings_file:
            json.dump({}, settings_file)
        result = subprocess.run(
            [sys.executable, str(self._script_path), str(settings_path), self._url],
            capture_output=True,
        )
        self.assertEqual(result.stderr, b"")


if __name__ == "__main__":
    unittest.main()
