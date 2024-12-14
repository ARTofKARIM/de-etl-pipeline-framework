"""Data loader implementations for various target destinations."""

import pandas as pd
from sqlalchemy import create_engine
import json
import os
from typing import Dict, Any

from src.base import BaseLoader


class CSVLoader(BaseLoader):
    """Loads data to CSV files."""

    def load(self, df: pd.DataFrame) -> int:
        self.validate_before_load(df)
        path = self.target_config["path"]
        options = self.target_config.get("options", {})

        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=options.get("index", False))
        self.rows_loaded = len(df)
        print(f"[CSVLoader] Loaded {self.rows_loaded} rows to {path}")
        return self.rows_loaded


class JSONLoader(BaseLoader):
    """Loads data to JSON files."""

    def load(self, df: pd.DataFrame) -> int:
        self.validate_before_load(df)
        path = self.target_config["path"]
        orient = self.target_config.get("options", {}).get("orient", "records")

        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_json(path, orient=orient, indent=2)
        self.rows_loaded = len(df)
        print(f"[JSONLoader] Loaded {self.rows_loaded} rows to {path}")
        return self.rows_loaded


class DatabaseLoader(BaseLoader):
    """Loads data to SQL databases via SQLAlchemy."""

    def load(self, df: pd.DataFrame) -> int:
        self.validate_before_load(df)
        conn_string = self.target_config["connection_string"]
        table = self.target_config["table"]
        if_exists = self.target_config.get("if_exists", "append")

        engine = create_engine(conn_string)
        df.to_sql(table, engine, if_exists=if_exists, index=False)
        self.rows_loaded = len(df)
        engine.dispose()
        print(f"[DatabaseLoader] Loaded {self.rows_loaded} rows to table '{table}'")
        return self.rows_loaded


class LoaderFactory:
    """Factory for creating loader instances."""

    _loaders = {
        "csv": CSVLoader,
        "json": JSONLoader,
        "database": DatabaseLoader,
    }

    @classmethod
    def create(cls, target_type: str, config: Dict[str, Any]) -> BaseLoader:
        if target_type not in cls._loaders:
            raise ValueError(f"Unknown target type: {target_type}")
        return cls._loaders[target_type](config)
