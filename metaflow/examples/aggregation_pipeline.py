"""
Aggregation pipeline example.

Demonstrates:
- Data aggregation patterns
- Window functions
- Statistical calculations
- Result caching
"""

from typing import Dict, Any, List
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, window, sum, avg, count, max

from metaflow.sources import FileSource
from metaflow.transformers import (
    AggregationTransformer,
    SchemaValidator,
    TypeCastTransformer
)
from metaflow.sinks import DeltaSink
from metaflow.runners import BatchRunner
from metaflow.utils.config_loader import load_config
from metaflow.utils.logger import setup_logger


def create_spark_session() -> SparkSession:
    """Create Spark session with required configuration."""
    return (SparkSession.builder
            .appName("AggregationPipelineExample")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def get_schema() -> StructType:
    """Define data schema."""
    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", TimestampType(), True)
    ])


def get_aggregations() -> List[Dict[str, Any]]:
    """Define aggregation specifications."""
    return [
        {
            "name": "daily_totals",
            "group_by": ["user_id"],
            "window": {
                "duration": "1 day",
                "slide": "1 day"
            },
            "metrics": [
                {"type": "sum", "column": "amount", "alias": "daily_spend"},
                {"type": "count", "column": "*", "alias": "transaction_count"}
            ]
        },
        {
            "name": "product_stats",
            "group_by": ["product_id"],
            "metrics": [
                {"type": "avg", "column": "amount", "alias": "avg_price"},
                {"type": "sum", "column": "amount", "alias": "total_revenue"},
                {"type": "count", "column": "*", "alias": "sales_count"}
            ]
        }
    ]


def main(config_path: str = "config.yaml"):
    """
    Run aggregation pipeline.
    
    Args:
        config_path: Path to configuration file
    """
    # Load configuration
    config = load_config(config_path)
    logger = setup_logger(__name__)
    
    # Initialize Spark
    spark = create_spark_session()
    logger.info("Initialized Spark session")
    
    try:
        # Create pipeline runner
        runner = BatchRunner(spark=spark)
        
        # Add source
        file_source = FileSource(
            spark=spark,
            path=config["input_path"],
            format="parquet",
            schema=get_schema()
        )
        runner.add_source("transactions", file_source)
        
        # Add schema validator
        schema_validator = SchemaValidator(
            spark=spark,
            config=config["schema_validator"],
            expected_schema=get_schema()
        )
        runner.add_transformer("validate_schema", schema_validator)
        
        # Add type casting
        type_caster = TypeCastTransformer(
            spark=spark,
            config=config["type_caster"],
            type_mappings={
                "amount": "double",
                "timestamp": "timestamp"
            }
        )
        runner.add_transformer("cast_types", type_caster)
        
        # Add aggregations
        aggregator = AggregationTransformer(
            spark=spark,
            config=config["aggregation"],
            aggregations=get_aggregations()
        )
        runner.add_transformer("aggregate", aggregator)
        
        # Add sink for aggregated results
        aggregated_sink = DeltaSink(
            spark=spark,
            path=config["output_path"],
            mode="overwrite"
        )
        runner.add_sink("aggregated_data", aggregated_sink)
        
        # Execute pipeline
        logger.info("Starting aggregation pipeline")
        results = runner.run()
        
        # Log aggregation metrics
        metrics = aggregator.get_metrics()
        logger.info(f"Aggregation metrics: {metrics}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Aggregation Pipeline Example")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file"
    )
    
    args = parser.parse_args()
    main(args.config)