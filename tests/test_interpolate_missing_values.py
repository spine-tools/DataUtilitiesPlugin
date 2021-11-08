import os.path
from tempfile import TemporaryDirectory
import unittest
from spinedb_api import (
    DatabaseMapping,
    from_database,
    import_object_classes,
    import_object_parameters,
    import_objects,
    import_object_parameter_values,
    import_relationship_classes,
    import_relationship_parameter_values,
    import_relationship_parameters,
    import_relationships,
    TimeSeriesFixedResolution,
    TimeSeriesVariableResolution,
)
from specifications.Tool.interpolate_missing_values import (
    fill,
    Interpolation,
    process_all,
    process_database,
)


class TestInterpolateMissingValues(unittest.TestCase):
    def test_linear_fill(self):
        time_series = TimeSeriesVariableResolution(
            ["2021-08-11T08:00", "2021-08-11T09:00", "2021-08-11T09:30"],
            [-30.0, -20.0, -10.0],
            False,
            False,
        )
        filled = fill(time_series, Interpolation.LINEAR)
        self.assertEqual(len(filled), 4)
        self.assertEqual(
            filled,
            TimeSeriesFixedResolution(
                "2021-08-11T08:00", "30m", [-30.0, -25.0, -20.0, -10.0], False, False
            ),
        )

    def test_next_fill(self):
        time_series = TimeSeriesVariableResolution(
            ["2021-08-11T08:00", "2021-08-11T09:00", "2021-08-11T09:30"],
            [-30.0, -20.0, -10.0],
            False,
            False,
        )
        filled = fill(time_series, Interpolation.NEXT)
        self.assertEqual(len(filled), 4)
        self.assertEqual(
            filled,
            TimeSeriesFixedResolution(
                "2021-08-11T08:00", "30m", [-30.0, -20.0, -20.0, -10.0], False, False
            ),
        )

    def test_previous_fill(self):
        time_series = TimeSeriesVariableResolution(
            ["2021-08-11T08:00", "2021-08-11T09:00", "2021-08-11T09:30"],
            [-30.0, -20.0, -10.0],
            False,
            False,
        )
        filled = fill(time_series, Interpolation.PREVIOUS)
        self.assertEqual(len(filled), 4)
        self.assertEqual(
            filled,
            TimeSeriesFixedResolution(
                "2021-08-11T08:00", "30m", [-30.0, -30.0, -20.0, -10.0], False, False
            ),
        )

    def test_nearest_fill(self):
        time_series = TimeSeriesVariableResolution(
            ["2021-08-11T08:00", "2021-08-11T09:30", "2021-08-11T10:00"],
            [-30.0, -20.0, -10.0],
            False,
            False,
        )
        filled = fill(time_series, Interpolation.NEAREST)
        self.assertEqual(len(filled), 5)
        self.assertEqual(
            filled,
            TimeSeriesFixedResolution(
                "2021-08-11T08:00",
                "30m",
                [-30.0, -30.0, -20.0, -20, 0 - 10.0],
                False,
                False,
            ),
        )

    def test_fill_keeps_flags(self):
        time_series = TimeSeriesVariableResolution(
            ["2021-08-11T08:00", "2021-08-11T09:00", "2021-08-11T09:30"],
            [-30.0, -20.0, -10.0],
            True,
            True,
        )
        filled = fill(time_series, Interpolation.LINEAR)
        self.assertTrue(filled.repeat)
        self.assertTrue(filled.ignore_year)

    def test_fill_object_time_series(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("class",))
        import_object_parameters(db_map, (("class", "param"),))
        import_objects(db_map, (("class", "obj"),))
        time_series = TimeSeriesVariableResolution(
            ["2021-08-11T08:00", "2021-08-11T09:00", "2021-08-11T09:30"],
            [-30.0, -20.0, -10.0],
            False,
            False,
        )
        import_object_parameter_values(
            db_map, (("class", "obj", "param", time_series),)
        )
        db_map.commit_session("Add test data.")
        update_data = process_database(db_map, Interpolation.LINEAR)
        self.assertEqual(len(update_data), 1)
        self.assertIn("id", update_data[0])
        self.assertIn("type", update_data[0])
        self.assertIn("value", update_data[0])
        filled = from_database(update_data[0]["value"], update_data[0]["type"])
        self.assertEqual(len(filled), 4)
        self.assertEqual(
            filled,
            TimeSeriesFixedResolution(
                "2021-08-11T08:00", "30m", [-30.0, -25.0, -20.0, -10.0], False, False
            ),
        )

    def test_fill_relationship_time_series(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("class",))
        import_objects(db_map, (("class", "obj"),))
        import_relationship_classes(db_map, (("rel_class", ("class",)),))
        import_relationship_parameters(db_map, (("rel_class", "param"),))
        import_relationships(db_map, (("rel_class", ("obj",)),))
        time_series = TimeSeriesVariableResolution(
            ["2021-08-11T08:00", "2021-08-11T09:00", "2021-08-11T09:30"],
            [-30.0, -20.0, -10.0],
            False,
            False,
        )
        import_relationship_parameter_values(db_map, (("rel_class", ("obj",), "param", time_series),))
        db_map.commit_session("Add test data.")
        update_data = process_database(db_map, Interpolation.LINEAR)
        self.assertEqual(len(update_data), 1)
        self.assertIn("id", update_data[0])
        self.assertIn("type", update_data[0])
        self.assertIn("value", update_data[0])
        filled = from_database(update_data[0]["value"], update_data[0]["type"])
        self.assertEqual(len(filled), 4)
        self.assertEqual(
            filled,
            TimeSeriesFixedResolution(
                "2021-08-11T08:00", "30m", [-30.0, -25.0, -20.0, -10.0], False, False
            ),
        )

    def test_process_real_database(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            db_map = DatabaseMapping(url, create=True)
            try:
                import_object_classes(db_map, ("class",))
                import_object_parameters(db_map, (("class", "param"),))
                import_objects(db_map, (("class", "obj"),))
                time_series = TimeSeriesVariableResolution(
                    ["2021-08-11T08:00", "2021-08-11T09:00", "2021-08-11T09:30"],
                    [-30.0, -20.0, -10.0],
                    False,
                    False,
                )
                import_object_parameter_values(
                    db_map, (("class", "obj", "param", time_series),)
                )
                db_map.commit_session("Add test data.")
                process_all([url], Interpolation.LINEAR)
                value_rows = db_map.query(db_map.object_parameter_value_sq).all()
                self.assertEqual(len(value_rows), 1)
                filled = from_database(value_rows[0].value, value_rows[0].type)
                self.assertEqual(len(filled), 4)
                self.assertEqual(
                    filled,
                    TimeSeriesFixedResolution(
                        "2021-08-11T08:00", "30m", [-30.0, -25.0, -20.0, -10.0], False, False
                    ),
                )
            finally:
                db_map.connection.close()


if __name__ == "__main__":
    unittest.main()
