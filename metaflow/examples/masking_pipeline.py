"""
Data masking pipeline example.

Demonstrates:
- PII data masking
- Encryption
- Data anonymization
- Custom masking rules
"""

from typing import Dict, Any, List
import hashlib

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, udf

from metaflow.sources import FileSource
from metaflow.transformers import (
    DataMaskingTransformer,
    SchemaValidator
)
from metaflow.sinks import DeltaSink
from metaflow.runners import BatchRunner
from metaflow.utils.config_loader import load_config
from metaflow.utils.logger import setup_logger


def create_spark_session() -> SparkSession:
    """Create Spark session with required configuration."""
    return (SparkSession.builder
            .appName("MaskingPipelineExample")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def get_schema() -> StructType:
    """Define data schema."""
    return StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("ssn", StringType(), True),
        StructField("credit_card", StringType(), True),
        StructField("address", StringType(), True)
    ])


def get_masking_rules() -> List[Dict[str, Any]]:
    """Define masking rules."""
    return [
        {
            "column": "email",
            "method": "hash",
            "parameters": {"salt": "email_salt"}
        },
        {
            "column": "ssn",
            "method": "pattern",
            "parameters": {"pattern": "XXX-XX-$4"}
        },
        {
            "column": "credit_card",
            "method": "pattern",
            "parameters": {"pattern": "XXXX-XXXX-XXXX-$4"}
        },
        {
            "column": "address",
            "method": "redact",
            "parameters": {"replacement": "[REDACTED]"}
        }
    ]


def main(config_path: str = "config.yaml"):
    """
    Run masking pipeline.
    
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
            format="csv",
            schema=get_schema()
        )
        runner.add_source("sensitive_data", file_source)
        
        # Add schema validator
        schema_validator = SchemaValidator(
            spark=spark,
            config=config["schema_validator"],
            expected_schema=get_schema()
        )
        runner.add_transformer("validate_schema", schema_validator)
        
        # Add data masking
        masking_transformer = DataMaskingTransformer(
            spark=spark,
            config=config["data_masking"],
            rules=get_masking_rules(),
            encryption_key=config["encryption_key"]
        )
        runner.add_transformer("mask_data", masking_transformer)
        
        # Add sink for masked data
        masked_sink = DeltaSink(
            spark=spark,
            path=config["output_path"],
            mode="overwrite"
        )
        runner.add_sink("masked_data", masked_sink)
        
        # Execute pipeline
        logger.info("Starting masking pipeline")
        runner.run()
        
        # Log masking metrics
        metrics = masking_transformer.get_masking_metrics()
        logger.info(f"Masking metrics: {metrics}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Masking Pipeline Example")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file"
    )
    
    args = parser.parse_args()
    main(args.config)