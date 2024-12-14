"""Concrete extractor implementations for various data sources."""

import pandas as pd
from sqlalchemy import create_engine, text
import json
import os
from typing import Dict, Any

from src.base import BaseExtractor


class CSVExtractor(BaseExtractor):
    """Extracts data from CSV files."""

    def __init__(self, source_config: Dict[str, Any]):
        super().__init__(source_config)

    def extract(self) -> pd.DataFrame:
        path = self.source_config["path"]
        options = self.source_config.get("options", {})
        encoding = options.get("encoding", "utf-8")
        delimiter = options.get("delimiter", ",")

        if not os.path.exists(path):
            raise FileNotFoundError(f"CSV file not found: {path}")

        self._data = pd.read_csv(path, encoding=encoding, delimiter=delimiter)
        self.validate(self._data)
        print(f"[CSVExtractor] Extracted {len(self._data)} rows from {path}")
        return self._data


class JSONExtractor(BaseExtractor):
    """Extracts data from JSON files (records format or nested)."""

    def __init__(self, source_config: Dict[str, Any]):
        super().__init__(source_config)

    def extract(self) -> pd.DataFrame:
        path = self.source_config["path"]
        orient = self.source_config.get("options", {}).get("orient", "records")

        if not os.path.exists(path):
            raise FileNotFoundError(f"JSON file not found: {path}")

        with open(path, "r") as f:
            raw = json.load(f)

        if isinstance(raw, list):
            self._data = pd.DataFrame(raw)
        elif isinstance(raw, dict):
            key = self.source_config.get("options", {}).get("data_key")
            data = raw[key] if key else raw
            self._data = pd.json_normalize(data)
        else:
            raise ValueError("Unsupported JSON structure")

        self.validate(self._data)
        print(f"[JSONExtractor] Extracted {len(self._data)} rows from {path}")
        return self._data


class DatabaseExtractor(BaseExtractor):
    """Extracts data from SQL databases using SQLAlchemy."""

    def __init__(self, source_config: Dict[str, Any]):
        super().__init__(source_config)
        self.engine = None

    def _get_engine(self):
        if self.engine is None:
            conn_string = self.source_config["connection_string"]
            self.engine = create_engine(conn_string)
        return self.engine

    def extract(self) -> pd.DataFrame:
        query = self.source_config.get("query", f"SELECT * FROM {self.source_config.get('table', 'data')}")
        engine = self._get_engine()

        with engine.connect() as conn:
            self._data = pd.read_sql(text(query), conn)

        self.validate(self._data)
        print(f"[DatabaseExtractor] Extracted {len(self._data)} rows")
        return self._data

    def close(self):
        if self.engine:
            self.engine.dispose()


class ExtractorFactory:
    """Factory for creating extractor instances."""

    _extractors = {
        "csv": CSVExtractor,
        "json": JSONExtractor,
        "database": DatabaseExtractor,
    }

    @classmethod
    def create(cls, source_type: str, config: Dict[str, Any]) -> BaseExtractor:
        if source_type not in cls._extractors:
            raise ValueError(f"Unknown source type: {source_type}. Available: {list(cls._extractors.keys())}")
        return cls._extractors[source_type](config)
