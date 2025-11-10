# MetaFlow Sinks

Data sink implementations for writing processed data to various targets.

## Available Sinks

### DeltaSink
Write to Delta Lake tables.

**Features:**
- Append, overwrite, merge operations
- Schema evolution
- Partition management
- Table optimization

**Example:**
```python
from metaflow.sinks import DeltaSink, SinkConfig, WriteMode

config = SinkConfig(
    write_mode=WriteMode.APPEND,
    partition_columns=['year', 'month'],
    enable_compression=True
)

sink = DeltaSink(spark, config, table_path='/data/delta/customers')
metrics = sink.write(df)

print(f"Wrote {metrics.records_written} records")