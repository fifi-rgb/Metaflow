# MetaFlow Runners

Pipeline execution engines for different workload types.

## Available Runners

### BatchRunner
Full table loads and batch processing.

**Use Cases:**
- Initial data migration
- Daily/hourly batch jobs
- Historical data loads
- One-time data exports

**Features:**
- Full and incremental batch loads
- Partition-based processing
- Delta Lake optimization
- Schema evolution

**Example:**
```python
from metaflow.runners import BatchRunner, RunnerConfig

config = RunnerConfig(
    spark_master="local[*]",
    shuffle_partitions=200,
    enable_checkpointing=True
)

runner = BatchRunner(config)

pipeline_config = {
    'pipeline_id': 'customer_batch_load',
    'source': {
        'type': 'jdbc',
        'url': 'jdbc:postgresql://localhost:5432/sourcedb',
        'table': 'customers',
        'user': 'metaflow',
        'password': 'password'
    },
    'target': '/data/delta/customers',
    'mode': 'overwrite',
    'partitions': ['country', 'signup_date']
}

metrics = runner.run(pipeline_config)
print(f"Processed {metrics.records_written} records")