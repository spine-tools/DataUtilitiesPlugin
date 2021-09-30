import unittest
import numpy
import xarray
from spinedb_api import DatabaseMapping, from_database, TimeSeriesVariableResolution

from specifications.Tool.atlite_time_series_reader import import_data_array, import_object_class, import_alternative, import_parameter


class TestDataArrayImport(unittest.TestCase):
    def setUp(self):
        pass

    def test_importing_data_array_as_object_parameter(self):
        data_array = xarray.DataArray(
            [[-2.3], [5.5]],
            coords=[
                [
                    numpy.datetime64("2021-09-29T09:00"),
                    numpy.datetime64("2021-09-29T10:00"),
                ],
                ["object1"],
            ],
            dims=["time", "object"],
        )
        db_map = DatabaseMapping("sqlite:///", create=True)
        try:
            import_object_class(db_map, "object_class")
            import_parameter(db_map, "object_class", "parameter1")
            import_alternative(db_map, "alternative1")
            import_data_array(db_map, data_array, "object_class", "parameter1", "alternative1")
            values = db_map.query(db_map.object_parameter_value_sq).all()
            self.assertEqual(len(values), 1)
            value = values[0]
            self.assertEqual(value.parameter_name, "parameter1")
            self.assertEqual(value.object_class_name, "object_class")
            self.assertEqual(value.object_name, "object1")
            self.assertEqual(value.alternative_name, "alternative1")
            self.assertEqual(value.type, "time_series")
            self.assertEqual(
                from_database(value.value, value.type),
                TimeSeriesVariableResolution(
                    ["2021-09-29T09:00", "2021-09-29T10:00"], [-2.3, 5.5], False, False
                ),
            )
        finally:
            db_map.connection.close()


if __name__ == "__main__":
    unittest.main()
