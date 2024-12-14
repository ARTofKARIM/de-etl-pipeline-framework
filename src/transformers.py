"""Configurable data transformation classes."""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional

from src.base import BaseTransformer


class DataCleaner(BaseTransformer):
    """Handles missing values and duplicates."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.validate_input(df)
        result = df.copy()
        strategy = self.params.get("strategy", "drop")
        subset = self.params.get("subset")

        initial_rows = len(result)

        # Remove duplicates
        if self.params.get("remove_duplicates", True):
            result = result.drop_duplicates()

        # Handle missing values
        if strategy == "drop":
            result = result.dropna(subset=subset)
        elif strategy == "fill_mean":
            numeric_cols = result.select_dtypes(include=[np.number]).columns
            cols = subset or numeric_cols
            result[cols] = result[cols].fillna(result[cols].mean())
        elif strategy == "fill_median":
            numeric_cols = result.select_dtypes(include=[np.number]).columns
            cols = subset or numeric_cols
            result[cols] = result[cols].fillna(result[cols].median())
        elif strategy == "fill_mode":
            cols = subset or result.columns
            for col in cols:
                if result[col].isnull().any():
                    result[col] = result[col].fillna(result[col].mode().iloc[0])
        elif strategy == "fill_value":
            fill_val = self.params.get("fill_value", 0)
            result = result.fillna(fill_val)

        removed = initial_rows - len(result)
        if removed > 0:
            print(f"[DataCleaner] Removed {removed} rows ({strategy})")
        return result


class TypeCaster(BaseTransformer):
    """Casts columns to specified data types."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.validate_input(df)
        result = df.copy()
        columns = self.params.get("columns", {})

        for col, dtype in columns.items():
            if col in result.columns:
                try:
                    if dtype == "datetime":
                        result[col] = pd.to_datetime(result[col])
                    else:
                        result[col] = result[col].astype(dtype)
                    print(f"[TypeCaster] Cast {col} to {dtype}")
                except (ValueError, TypeError) as e:
                    print(f"[TypeCaster] Warning: Could not cast {col} to {dtype}: {e}")
        return result


class ColumnMapper(BaseTransformer):
    """Renames columns according to a mapping."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.validate_input(df)
        mapping = self.params.get("mapping", {})
        result = df.rename(columns=mapping)
        print(f"[ColumnMapper] Renamed {len(mapping)} columns")
        return result


class FilterTransformer(BaseTransformer):
    """Filters rows based on column conditions."""

    _operators = {
        ">": lambda s, v: s > v,
        "<": lambda s, v: s < v,
        ">=": lambda s, v: s >= v,
        "<=": lambda s, v: s <= v,
        "==": lambda s, v: s == v,
        "!=": lambda s, v: s != v,
        "in": lambda s, v: s.isin(v),
        "not_in": lambda s, v: ~s.isin(v),
    }

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.validate_input(df)
        col = self.params["column"]
        op = self.params["operator"]
        value = self.params["value"]

        if op not in self._operators:
            raise ValueError(f"Unknown operator: {op}")

        mask = self._operators[op](df[col], value)
        result = df[mask].reset_index(drop=True)
        removed = len(df) - len(result)
        print(f"[FilterTransformer] Filtered {col} {op} {value}: removed {removed} rows")
        return result


class TransformerFactory:
    """Factory for creating transformer instances."""

    _transformers = {
        "DataCleaner": DataCleaner,
        "TypeCaster": TypeCaster,
        "ColumnMapper": ColumnMapper,
        "FilterTransformer": FilterTransformer,
    }

    @classmethod
    def create(cls, transformer_type: str, params: Dict[str, Any] = None) -> BaseTransformer:
        if transformer_type not in cls._transformers:
            raise ValueError(f"Unknown transformer: {transformer_type}")
        return cls._transformers[transformer_type](params=params)
