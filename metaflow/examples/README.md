# MetaFlow Pipeline Examples

This directory contains complete, ready-to-run examples of various data pipeline patterns using MetaFlow.

## Examples Overview

| Example | Description | Use Case |
|---------|-------------|----------|
| `batch_pipeline.py` | Full batch processing | Daily/hourly data loads |
| `streaming_pipeline.py` | Real-time streaming | Event processing |
| `incremental_pipeline.py` | CDC processing | Change data capture |
| `quality_pipeline.py` | Data quality validation | Data cleansing |
| `masking_pipeline.py` | PII protection | Compliance/GDPR |
| `multi_source_pipeline.py` | Multiple sources | Data integration |
| `aggregation_pipeline.py` | Data aggregation | Analytics/reporting |
| `end_to_end_pipeline.py` | Complete ETL | Production pipeline |

## Prerequisites

```bash
# Install dependencies
pip install pyspark delta-spark kafka-python requests

# Start local services (optional)
docker-compose up -d  # PostgreSQL, Kafka, etc.