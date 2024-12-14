"""Unit tests for data extractors."""

import unittest
import pandas as pd
import os
import json
import tempfile

from src.extractors import CSVExtractor, JSONExtractor


class TestCSVExtractor(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.csv_path = os.path.join(self.tmpdir, "test.csv")
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [10.0, 20.0, 30.0]})
        df.to_csv(self.csv_path, index=False)

    def test_extract_csv(self):
        config = {"path": self.csv_path, "options": {"encoding": "utf-8", "delimiter": ","}}
        extractor = CSVExtractor(config)
        df = extractor.extract()
        self.assertEqual(len(df), 3)
        self.assertListEqual(list(df.columns), ["id", "name", "value"])

    def test_file_not_found(self):
        config = {"path": "/nonexistent/file.csv"}
        extractor = CSVExtractor(config)
        with self.assertRaises(FileNotFoundError):
            extractor.extract()


class TestJSONExtractor(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.json_path = os.path.join(self.tmpdir, "test.json")
        data = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        with open(self.json_path, "w") as f:
            json.dump(data, f)

    def test_extract_json_records(self):
        config = {"path": self.json_path}
        extractor = JSONExtractor(config)
        df = extractor.extract()
        self.assertEqual(len(df), 2)
        self.assertIn("id", df.columns)


if __name__ == "__main__":
    unittest.main()
