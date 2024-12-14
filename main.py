"""Entry point for the ETL pipeline framework."""

import argparse
import sys
from loguru import logger
from src.pipeline import Pipeline


def setup_logging(log_level: str = "INFO"):
    """Configure loguru logging."""
    logger.remove()
    logger.add(sys.stderr, level=log_level, format="{time:HH:mm:ss} | {level:<7} | {message}")
    logger.add("logs/pipeline_{time}.log", rotation="10 MB", level="DEBUG")


def main():
    parser = argparse.ArgumentParser(description="ETL Pipeline Framework")
    parser.add_argument("--config", type=str, default="config/pipeline_config.yaml",
                        help="Path to pipeline configuration YAML")
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--retries", type=int, default=3, help="Max retry attempts")
    args = parser.parse_args()

    setup_logging(args.log_level)
    logger.info(f"Loading pipeline from {args.config}")

    pipeline = Pipeline.from_config(args.config)
    result = pipeline.run(max_retries=args.retries)

    print(f"\nPipeline Result:")
    print(f"  Status: {result['status']}")
    print(f"  Rows extracted: {result['rows_extracted']}")
    print(f"  Rows loaded: {result['rows_loaded']}")
    print(f"  Duration: {result.get('duration_seconds', 'N/A')}s")
    if result["errors"]:
        print(f"  Errors: {result['errors']}")

    sys.exit(0 if result["status"] == "success" else 1)


if __name__ == "__main__":
    main()
