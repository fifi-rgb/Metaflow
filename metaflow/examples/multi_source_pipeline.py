"""
Multi-source pipeline example.

Demonstrates:
- Multiple data source integration
- Join operations
- Data enrichment
- Error handling
"""

from typing import Dict, Any
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, when

from metaflow.sources import FileSource, JDBCSource, APISource
from metaflow.transformers import (
    SchemaValidator,
    DataQualityTransformer,
    TypeCastTransformer
)
from metaflow.sinks import DeltaSink
from metaflow.runners import BatchRunner
from metaflow.utils.config_loader import load_config
from metaflow.utils.logger import setup_logger


def create_spark_session() -> SparkSession:
    """Create Spark session with required configuration."""
    return (SparkSession.builder
            .appName("MultiSourcePipelineExample")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def get_user_schema() -> StructType:
    """Define user data schema."""
    return StructType([
        StructField("user_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True)
    ])


def get_order_schema() -> StructType:
    """Define order data schema."""
    return StructType([
        StructField("order_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("total", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ])


def get_product_schema() -> StructType:
    """Define product data schema."""
    return StructType([
        StructField("product_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", StringType(), True)
    ])


def main(config_path: str = "config.yaml"):
    """
    Run multi-source pipeline.
    
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
        
        # Add user source (CSV file)
        user_source = FileSource(
            spark=spark,
            path=config["user_data_path"],
            format="csv",
            schema=get_user_schema()
        )
        runner.add_source("users", user_source)
        
        # Add order source (JDBC)
        order_source = JDBCSource(
            spark=spark,
            url=config["jdbc_url"],
            table=config["orders_table"],
            properties=config["jdbc_properties"]
        )
        runner.add_source("orders", order_source)
        
        # Add product source (API)
        product_source = APISource(
            spark=spark,
            url=config["product_api_url"],
            headers=config["api_headers"],
            schema=get_product_schema()
        )
        runner.add_source("products", product_source)
        
        # Add transformers for each source
        user_validator = SchemaValidator(
            spark=spark,
            config=config["schema_validator"],
            expected_schema=get_user_schema()
        )
        runner.add_transformer("validate_users", user_validator)
        
        order_validator = SchemaValidator(
            spark=spark,
            config=config["schema_validator"],
            expected_schema=get_order_schema()
        )
        runner.add_transformer("validate_orders", order_validator)
        
        product_validator = SchemaValidator(
            spark=spark,
            config=config["schema_validator"],
            expected_schema=get_product_schema()
        )
        runner.add_transformer("validate_products", product_validator)
        
        # Add type casting
        type_caster = TypeCastTransformer(
            spark=spark,
            config=config["type_caster"],
            type_mappings=config["type_mappings"]
        )
        runner.add_transformer("cast_types", type_caster)
        
        # Add data quality checks
        quality_checker = DataQualityTransformer(
            spark=spark,
            config=config["data_quality"],
            rules=config["quality_rules"]
        )
        runner.add_transformer("check_quality", quality_checker)
        
        # Add sink for combined data
        combined_sink = DeltaSink(
            spark=spark,
            path=config["output_path"],
            mode="overwrite"
        )
        runner.add_sink("combined_data", combined_sink)
        
        # Execute pipeline
        logger.info("Starting multi-source pipeline")
        runner.run()
        logger.info("Pipeline execution completed")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Multi-source Pipeline Example")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file"
    )
    
    args = parser.parse_args()
    main(args.config)