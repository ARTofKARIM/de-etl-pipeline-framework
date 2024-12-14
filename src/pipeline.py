"""Pipeline orchestrator for chaining ETL operations."""

import yaml
import time
from typing import List, Optional
from loguru import logger

from src.extractors import ExtractorFactory
from src.transformers import TransformerFactory
from src.loaders import LoaderFactory


class Pipeline:
    """Orchestrates the full ETL pipeline from config or programmatic setup."""

    def __init__(self, name: str = "default"):
        self.name = name
        self.extractor = None
        self.transformers = []
        self.loader = None
        self.execution_log = []

    @classmethod
    def from_config(cls, config_path: str) -> "Pipeline":
        """Create a pipeline from a YAML configuration file."""
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        pipeline_config = config.get("pipeline", {})
        pipe = cls(name=pipeline_config.get("name", "config_pipeline"))

        # Setup extractor
        source = config["source"]
        pipe.extractor = ExtractorFactory.create(source["type"], source)

        # Setup transformers
        for t_config in config.get("transformations", []):
            transformer = TransformerFactory.create(
                t_config["type"], params=t_config.get("params", {})
            )
            pipe.transformers.append(transformer)

        # Setup loader
        target = config["target"]
        pipe.loader = LoaderFactory.create(target["type"], target)

        return pipe

    def add_transformer(self, transformer):
        """Add a transformer to the pipeline."""
        self.transformers.append(transformer)
        return self

    def run(self, max_retries: int = 3) -> dict:
        """Execute the full ETL pipeline with retry logic."""
        start_time = time.time()
        logger.info(f"Starting pipeline: {self.name}")

        result = {"status": "success", "rows_extracted": 0, "rows_loaded": 0, "errors": []}

        # Extract
        for attempt in range(max_retries):
            try:
                df = self.extractor.extract()
                result["rows_extracted"] = len(df)
                logger.info(f"Extracted {len(df)} rows")
                break
            except Exception as e:
                logger.warning(f"Extraction attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    result["status"] = "failed"
                    result["errors"].append(f"Extraction failed: {e}")
                    return result

        # Transform
        for transformer in self.transformers:
            try:
                rows_before = len(df)
                df = transformer.transform(df)
                logger.info(f"{transformer.name}: {rows_before} -> {len(df)} rows")
            except Exception as e:
                logger.error(f"Transformation {transformer.name} failed: {e}")
                result["errors"].append(f"{transformer.name}: {e}")
                result["status"] = "partial"

        # Load
        try:
            rows_loaded = self.loader.load(df)
            result["rows_loaded"] = rows_loaded
            logger.info(f"Loaded {rows_loaded} rows")
        except Exception as e:
            logger.error(f"Loading failed: {e}")
            result["status"] = "failed"
            result["errors"].append(f"Loading failed: {e}")

        elapsed = time.time() - start_time
        result["duration_seconds"] = round(elapsed, 2)
        logger.info(f"Pipeline '{self.name}' completed in {elapsed:.2f}s - Status: {result['status']}")
        return result
