# MetaFlow - Enterprise Data Pipeline Framework

MetaFlow is a comprehensive PySpark-based framework for building scalable data pipelines with support for batch, streaming, and incremental processing.

## Features

### 🔌 Data Sources
- **JDBC**: PostgreSQL, MySQL, Oracle, SQL Server
- **Delta Lake**: Native Delta table support
- **Files**: Parquet, CSV, JSON, Avro, ORC
- **Kafka**: Real-time streaming
- **REST APIs**: HTTP/REST endpoints

### 📊 Data Sinks
- **Delta Lake**: ACID transactions, time travel
- **JDBC**: Batch writes to databases
- **Files**: Multiple format support
- **Kafka**: Event streaming
- **REST APIs**: HTTP POST/PUT operations

### 🔄 Pipeline Runners
- **BatchRunner**: Full table loads and batch processing
- **StreamingRunner**: Real-time Kafka/Kinesis streaming
- **IncrementalRunner**: CDC and incremental updates

### 🛠️ Data Transformers
- **DataQualityTransformer**: Validation and cleansing
- **Deduplicator**: Duplicate removal
- **SchemaValidator**: Schema validation and evolution
- **DataMaskingTransformer**: PII protection
- **AggregationTransformer**: Data aggregation
- **TypeCaster**: Type conversion

## Quick Start

### Installation

```bash
pip install pyspark delta-spark