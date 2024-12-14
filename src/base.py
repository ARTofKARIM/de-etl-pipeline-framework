"""Abstract base classes for ETL pipeline components."""

from abc import ABC, abstractmethod
import pandas as pd
from typing import Any, Dict, Optional


class BaseExtractor(ABC):
    """Abstract base class for data extractors."""

    def __init__(self, source_config: Dict[str, Any]):
        self.source_config = source_config
        self._data = None

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extract data from the source and return as DataFrame."""
        pass

    def validate(self, df: pd.DataFrame) -> bool:
        """Basic validation that extraction produced results."""
        if df is None or df.empty:
            raise ValueError("Extraction produced empty DataFrame")
        return True

    @property
    def data(self) -> Optional[pd.DataFrame]:
        return self._data


class BaseTransformer(ABC):
    """Abstract base class for data transformers."""

    def __init__(self, params: Dict[str, Any] = None):
        self.params = params or {}
        self.name = self.__class__.__name__

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply transformation to the DataFrame."""
        pass

    def validate_input(self, df: pd.DataFrame) -> bool:
        if df is None:
            raise ValueError(f"{self.name}: Input DataFrame is None")
        return True


class BaseLoader(ABC):
    """Abstract base class for data loaders."""

    def __init__(self, target_config: Dict[str, Any]):
        self.target_config = target_config
        self.rows_loaded = 0

    @abstractmethod
    def load(self, df: pd.DataFrame) -> int:
        """Load DataFrame to the target destination. Returns row count."""
        pass

    def validate_before_load(self, df: pd.DataFrame) -> bool:
        if df is None or df.empty:
            raise ValueError("Cannot load empty DataFrame")
        return True
