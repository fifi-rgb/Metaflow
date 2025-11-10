"""
Batch Pipeline Example

Demonstrates:
- Loading data from JDBC source
- Applying transformations
- Writing to Delta Lake
- Monitoring and metrics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

from metaflow.sources.jdbc_source import JdbcSource, SourceConfig
from metaflow.sinks.delta import DeltaSink, SinkConfig, WriteMode
from metaflow.runners.batch_runner import BatchRunner, RunnerConfig
from metaflow.transformers.data_quality import DataQualityTransformer, QualityRule, RuleType, TransformerConfig


def create_spark_session():
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName("MetaFlow-Batch-Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def main():
    """Run batch pipeline example."""
    print("=" * 80)
    print("MetaFlow Batch Pipeline Example")
    print("=" * 80)
    
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        # Step 1: Configure source
        print("\n📥 Step 1: Configuring JDBC Source...")
        source_config = SourceConfig(
            connection_url="jdbc:postgresql://localhost:5432/sourcedb",
            table="customers",
            user="postgres",
            password="password",
            driver="org.postgresql.Driver",
            fetch_size=10000,
            partition_column="customer_id",
            num_partitions=4
        )
        
        source = JdbcSource(spark, source_config)
        
        # Step 2: Load data
        print("\n📊 Step 2: Loading data from source...")
        df = source.read()
        print(f"   Loaded {df.count()} records")
        print(f"   Schema: {df.columns}")
        
        # Step 3: Apply data quality transformations
        print("\n🔍 Step 3: Applying data quality checks...")
        
        quality_config = TransformerConfig(enable_metrics=True)
        quality_rules = [
            QualityRule(
                name="email_not_null",
                column="email",
                rule_type=RuleType.NOT_NULL,
                parameters={},
                severity="error"
            ),
            QualityRule(
                name="email_format",
                column="email",
                rule_type=RuleType.REGEX,
                parameters={'pattern': r'^[\w\.-]+@[\w\.-]+\.\w+$'},
                severity="error"
            ),
            QualityRule(
                name="age_range",
                column="age",
                rule_type=RuleType.RANGE,
                parameters={'min': 18, 'max': 120},
                severity="warning"
            ),
            QualityRule(
                name="country_valid",
                column="country",
                rule_type=RuleType.IN_LIST,
                parameters={'values': ['US', 'UK', 'CA', 'AU', 'DE']},
                severity="error"
            )
        ]
        
        quality_transformer = DataQualityTransformer(
            spark,
            quality_config,
            rules=quality_rules,
            quarantine_invalid=True,
            quarantine_path="/tmp/quarantine/customers"
        )
        
        cleaned_df = quality_transformer.transform(df)
        
        # Get quality metrics
        metrics = quality_transformer.get_quality_metrics()
        print(f"\n   Quality Metrics:")
        print(f"   - Total records: {metrics['total_records']}")
        print(f"   - Valid records: {metrics['valid_records']}")
        print(f"   - Invalid records: {metrics['invalid_records']}")
        print(f"   - Quality score: {metrics['quality_score']:.2f}%")
        print(f"   - Failed rules: {metrics['failed_rules']}")
        
        # Step 4: Add processing metadata
        print("\n🏷️  Step 4: Adding metadata...")
        final_df = cleaned_df.withColumn("processed_at", current_timestamp())
        
        # Step 5: Write to Delta Lake
        print("\n💾 Step 5: Writing to Delta Lake...")
        sink_config = SinkConfig(
            write_mode=WriteMode.OVERWRITE,
            partition_columns=['country'],
            enable_compression=True,
            compression_codec='snappy',
            enable_optimize=True,
            enable_vacuum=True,
            vacuum_retention_hours=168
        )
        
        sink = DeltaSink(spark, sink_config)
        sink.write(final_df, "/tmp/delta/customers")
        
        write_metrics = sink.get_write_metrics()
        print(f"\n   Write Metrics:")
        print(f"   - Records written: {write_metrics['records_written']}")
        print(f"   - Files written: {write_metrics['files_written']}")
        print(f"   - Partitions: {write_metrics['partitions']}")
        
        # Step 6: Verify write
        print("\n✅ Step 6: Verifying data...")
        verification_df = spark.read.format("delta").load("/tmp/delta/customers")
        print(f"   Verified {verification_df.count()} records in Delta table")
        
        print("\n" + "=" * 80)
        print("✨ Batch Pipeline Completed Successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()