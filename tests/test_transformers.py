"""Unit tests for data transformers."""

import unittest
import pandas as pd
import numpy as np

from src.transformers import DataCleaner, TypeCaster, FilterTransformer, ColumnMapper


class TestDataCleaner(unittest.TestCase):

    def test_drop_nulls(self):
        df = pd.DataFrame({"a": [1, None, 3], "b": [4, 5, None]})
        cleaner = DataCleaner(params={"strategy": "drop"})
        result = cleaner.transform(df)
        self.assertEqual(len(result), 1)

    def test_fill_mean(self):
        df = pd.DataFrame({"a": [1.0, None, 3.0], "b": [4.0, 5.0, 6.0]})
        cleaner = DataCleaner(params={"strategy": "fill_mean"})
        result = cleaner.transform(df)
        self.assertAlmostEqual(result["a"].iloc[1], 2.0)

    def test_remove_duplicates(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": [3, 3, 4]})
        cleaner = DataCleaner(params={"strategy": "drop", "remove_duplicates": True})
        result = cleaner.transform(df)
        self.assertEqual(len(result), 2)


class TestTypeCaster(unittest.TestCase):

    def test_cast_to_float(self):
        df = pd.DataFrame({"a": ["1", "2", "3"]})
        caster = TypeCaster(params={"columns": {"a": "float64"}})
        result = caster.transform(df)
        self.assertEqual(result["a"].dtype, np.float64)


class TestFilterTransformer(unittest.TestCase):

    def test_greater_than_filter(self):
        df = pd.DataFrame({"value": [10, 20, 30, 40]})
        filt = FilterTransformer(params={"column": "value", "operator": ">", "value": 20})
        result = filt.transform(df)
        self.assertEqual(len(result), 2)

    def test_equals_filter(self):
        df = pd.DataFrame({"status": ["a", "b", "a", "c"]})
        filt = FilterTransformer(params={"column": "status", "operator": "==", "value": "a"})
        result = filt.transform(df)
        self.assertEqual(len(result), 2)


if __name__ == "__main__":
    unittest.main()
