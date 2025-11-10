# MetaFlow Sources

This package provides various data source implementations for MetaFlow.

## Available Sources

- `APISource`: Read data from REST APIs with pagination and authentication support
- `BaseSource`: Abstract base class for all sources
- `DeltaSource`: Read from Delta Lake tables with time travel capabilities
- `FileSource`: Read various file formats (CSV, JSON, Parquet, etc.)
- `JDBCSource`: Read from relational databases
- `KafkaSource`: Read from Apache Kafka topics

## Usage

Each source follows a similar pattern:

```python
from metaflow.sources import FileSource

# Create source instance
source = FileSource(
    spark=spark,
    path="data/input",
    format="parquet"
)

# Read data
df = source.read()
```

## Creating Custom Sources

Extend the `BaseSource` class to create custom sources:

```python
from metaflow.sources import BaseSource

class CustomSource(BaseSource):
    def __init__(self, spark, **kwargs):
        super().__init__(spark)
        self.config = kwargs
        
    def read(self):
        # Implement read logic here
        pass
```