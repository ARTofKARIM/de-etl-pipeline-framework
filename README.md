# ETL Pipeline Framework

A modular, configurable ETL (Extract, Transform, Load) pipeline framework built in Python. Supports multiple data sources and destinations with a plugin-based architecture.

## Architecture

```
de-etl-pipeline-framework/
├── src/
│   ├── base.py           # Abstract base classes (Extractor, Transformer, Loader)
│   ├── extractors.py     # CSV, JSON, Database extractors
│   ├── transformers.py   # DataCleaner, TypeCaster, Filter, ColumnMapper
│   ├── loaders.py        # CSV, JSON, Database loaders
│   ├── pipeline.py       # Pipeline orchestrator with retry logic
│   └── logger.py         # Centralized logging with loguru
├── config/
│   └── pipeline_config.yaml  # Pipeline configuration
├── tests/
│   ├── test_extractors.py
│   └── test_transformers.py
└── main.py               # CLI entry point
```

## Design Pattern

The framework uses the **Factory Pattern** for creating ETL components and a **Pipeline Pattern** for chaining operations:

```
Extractor → [Transformer₁ → Transformer₂ → ... → Transformerₙ] → Loader
```

## Supported Sources & Targets

| Type | Extractor | Loader |
|------|-----------|--------|
| CSV | CSVExtractor | CSVLoader |
| JSON | JSONExtractor | JSONLoader |
| SQL Database | DatabaseExtractor | DatabaseLoader |

## Installation

```bash
git clone https://github.com/mouachiqab/de-etl-pipeline-framework.git
cd de-etl-pipeline-framework
pip install -r requirements.txt
```

## Usage

### From Configuration File
```bash
python main.py --config config/pipeline_config.yaml
```

### Programmatic Usage
```python
from src.pipeline import Pipeline
from src.extractors import CSVExtractor
from src.transformers import DataCleaner, TypeCaster
from src.loaders import CSVLoader

pipe = Pipeline(name="my_pipeline")
pipe.extractor = CSVExtractor({"path": "data/input.csv"})
pipe.add_transformer(DataCleaner(params={"strategy": "fill_mean"}))
pipe.add_transformer(TypeCaster(params={"columns": {"amount": "float64"}}))
pipe.loader = CSVLoader({"path": "data/output.csv"})
result = pipe.run()
```

## Technologies

- Python 3.9+
- pandas, SQLAlchemy
- loguru (logging)
- PyYAML (configuration)
- pytest (testing)











