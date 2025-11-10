"""
End-to-end pipeline example.

Demonstrates:
- Complete data pipeline workflow
- Multiple transformations
- Error handling
- Monitoring integration
"""

from typing import Dict, Any
from datetime import datetime
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, current_timestamp

from metaflow.sources import FileSource, JDBCSource
from metaflow.transformers import (
    SchemaValidator,
    DataQualityTransformer,
    DataMaskingTransformer,
    AggregationTransformer,
    DeduplicationTransformer
)
from metaflow.sinks import DeltaSink, KafkaSink
from metaflow.runners import BatchRunner
from metaflow.utils.config_loader import load_config
from metaflow.utils.logger import setup_logger


def create_spark_session() -> SparkSession:
    """Create Spark session with required configuration."""
    return (SparkSession.builder
            .appName("EndToEndPipelineExample")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def get_input_schema() -> StructType:
    """Define input data schema."""
    return StructType([
        StructField("id", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("data", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])


def main(config_path: str = "config.yaml"):
    """
    Run end-to-end pipeline.
    
    Args:
        config_path: Path to configuration file
    """
    # Load configuration
    config = load_config(config_path)
    logger = setup_logger(__name__)
    
    # Initialize Spark
    spark = create_spark_session()
    logger.info("Initialized Spark session")
    
    start_time = time.time()
    
    try:
        # Create pipeline runner
        runner = BatchRunner(spark=spark)
        
        # Add sources
        file_source = FileSource(
            spark=spark,
            path=config["input_path"],
            format="parquet",
            schema=get_input_schema()
        )
        runner.add_source("raw_data", file_source)
        
        jdbc_source = JDBCSource(
            spark=spark,
            url=config["jdbc_url"],
            table=config["lookup_table"],
            properties=config["jdbc_properties"]
        )
        runner.add_source("lookup_data", jdbc_source)
        
        # Add schema validation
        schema_validator = SchemaValidator(
            spark=spark,
            config=config["schema_validator"],
            expected_schema=get_input_schema()
        )
        runner.add_transformer("validate_schema", schema_validator)
        
        # Add data quality checks
        quality_checker = DataQualityTransformer(
            spark=spark,
            config=config["data_quality"],
            rules=config["quality_rules"],
            quarantine_invalid=True,
            quarantine_path=config["quarantine_path"]
        )
        runner.add_transformer("check_quality", quality_checker)
        
        # Add data masking
        masking_transformer = DataMaskingTransformer(
            spark=spark,
            config=config["data_masking"],
            rules=config["masking_rules"]
        )
        runner.add_transformer("mask_data", masking_transformer)
        
        # Add deduplication
        deduplicator = DeduplicationTransformer(
            spark=spark,
            config=config["deduplication"],
            key_columns=["id"],
            timestamp_column="timestamp"
        )
        runner.add_transformer("deduplicate", deduplicator)
        
        # Add aggregations
        aggregator = AggregationTransformer(
            spark=spark,
            config=config["aggregation"],
            aggregations=config["aggregations"]
        )
        runner.add_transformer("aggregate", aggregator)
        
        # Add sinks
        delta_sink = DeltaSink(
            spark=spark,
            path=config["output_path"],
            mode="overwrite"
        )
        runner.add_sink("processed_data", delta_sink)
        
        # Add Kafka sink for notifications
        kafka_sink = KafkaSink(
            spark=spark,
            topic=config["notification_topic"],
            bootstrap_servers=config["kafka_bootstrap_servers"]
        )
        runner.add_sink("notifications", kafka_sink)
        
        # Execute pipeline
        logger.info("Starting end-to-end pipeline")
        results = runner.run()
        
        # Collect metrics
        end_time = time.time()
        duration = end_time - start_time
        
        pipeline_metrics = {
            "pipeline_name": "end_to_end_pipeline",
            "start_time": datetime.fromtimestamp(start_time).isoformat(),
            "end_time": datetime.fromtimestamp(end_time).isoformat(),
            "duration_seconds": duration,
            "records_processed": results.get("processed_data", {}).get("count", 0),
            "quality_metrics": quality_checker.get_quality_metrics(),
            "masking_metrics": masking_transformer.get_masking_metrics(),
            "dedup_metrics": deduplicator.get_duplicate_count(),
            "aggregation_metrics": aggregator.get_metrics()
        }
        
        logger.info(f"Pipeline metrics: {pipeline_metrics}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
        
    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="End-to-end Pipeline Example")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file"
    )
    
    args = parser.parse_args()
    main(args.config)