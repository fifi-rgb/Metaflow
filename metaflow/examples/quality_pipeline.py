"""
Data Quality Pipeline Example

Demonstrates:
- Comprehensive data quality checks
- Quarantine handling
- Quality reporting
- Auto-correction
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, regexp_replace, current_timestamp
from datetime import datetime

from metaflow.sources.file_source import FileSource, FileSourceConfig, FileFormat
from metaflow.transformers.data_quality import (
    DataQualityTransformer,
    QualityRule,
    RuleType,
    TransformerConfig
)
from metaflow.sinks.delta import DeltaSink, SinkConfig, WriteMode


def create_spark_session():
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName("MetaFlow-Quality-Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def create_sample_data(spark):
    """Create sample data with quality issues."""
    data = [
        ("1", "john.doe@email.com", 25, "US", "2024-01-01", 1000.00),
        ("2", "jane.smith@email.com", 30, "UK", "2024-01-02", 1500.50),
        ("3", "invalid-email", 150, "XX", "2024-01-03", -100.00),  # Bad: email, age, country, amount
        ("4", None, 28, "CA", "2024-01-04", 2000.00),  # Bad: null email
        ("5", "bob@email.com", -5, "US", None, 500.00),  # Bad: negative age, null date
        ("6", "alice@email.com", 22, "DE", "2024-01-06", 750.25),
        ("7", "charlie@email.com", 45, "AU", "2024-01-07", 3000.00),
        ("8", "", 35, "US", "2024-01-08", 1200.00),  # Bad: empty email
        ("9", "david@email.com", 60, "FR", "2024-01-09", 1800.00),  # Bad: FR not in list
        ("10", "eve@email.com", 29, "UK", "2024-01-10", 999999.00),  # Bad: amount too high
    ]
    
    columns = ["user_id", "email", "age", "country", "signup_date", "account_balance"]
    return spark.createDataFrame(data, columns)


def main():
    """Run data quality pipeline example."""
    print("=" * 80)
    print("MetaFlow Data Quality Pipeline Example")
    print("=" * 80)
    
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        # Step 1: Load sample data
        print("\n📥 Step 1: Loading sample data...")
        df = create_sample_data(spark)
        print(f"   Loaded {df.count()} records")
        
        print("\n   Sample data (with intentional quality issues):")
        df.show(truncate=False)
        
        # Step 2: Define comprehensive quality rules
        print("\n📋 Step 2: Defining quality rules...")
        
        quality_rules = [
            # Email validation
            QualityRule(
                name="email_not_null",
                column="email",
                rule_type=RuleType.NOT_NULL,
                parameters={},
                severity="error",
                description="Email must not be null"
            ),
            QualityRule(
                name="email_not_empty",
                column="email",
                rule_type=RuleType.CUSTOM,
                parameters={'condition': "length(trim(email)) > 0"},
                severity="error",
                description="Email must not be empty"
            ),
            QualityRule(
                name="email_format",
                column="email",
                rule_type=RuleType.REGEX,
                parameters={'pattern': r'^[\w\.-]+@[\w\.-]+\.\w+$'},
                severity="error",
                description="Email must be valid format"
            ),
            
            # Age validation
            QualityRule(
                name="age_not_null",
                column="age",
                rule_type=RuleType.NOT_NULL,
                parameters={},
                severity="error"
            ),
            QualityRule(
                name="age_range",
                column="age",
                rule_type=RuleType.RANGE,
                parameters={'min': 18, 'max': 120},
                severity="error",
                description="Age must be between 18 and 120"
            ),
            
            # Country validation
            QualityRule(
                name="country_valid",
                column="country",
                rule_type=RuleType.IN_LIST,
                parameters={'values': ['US', 'UK', 'CA', 'AU', 'DE']},
                severity="error",
                description="Country must be in allowed list"
            ),
            
            # Date validation
            QualityRule(
                name="signup_date_not_null",
                column="signup_date",
                rule_type=RuleType.NOT_NULL,
                parameters={},
                severity="warning"
            ),
            
            # Amount validation
            QualityRule(
                name="balance_positive",
                column="account_balance",
                rule_type=RuleType.RANGE,
                parameters={'min': 0.0, 'max': 100000.0},
                severity="error",
                description="Account balance must be between 0 and 100,000"
            ),
        ]
        
        print(f"   Defined {len(quality_rules)} quality rules")
        
        # Step 3: Apply quality transformation
        print("\n🔍 Step 3: Applying quality checks...")
        
        config = TransformerConfig(enable_metrics=True)
        
        quality_transformer = DataQualityTransformer(
            spark,
            config,
            rules=quality_rules,
            quarantine_invalid=True,
            quarantine_path="/tmp/quarantine/users",
            add_quality_columns=True
        )
        
        cleaned_df = quality_transformer.transform(df)
        
        # Step 4: Analyze results
        print("\n📊 Step 4: Analyzing quality results...")
        
        metrics = quality_transformer.get_quality_metrics()
        
        print(f"\n   {'='*76}")
        print(f"   QUALITY METRICS SUMMARY")
        print(f"   {'='*76}")
        print(f"   Total Records:        {metrics['total_records']}")
        print(f"   Valid Records:        {metrics['valid_records']} ({metrics['quality_score']:.2f}%)")
        print(f"   Invalid Records:      {metrics['invalid_records']}")
        print(f"   {'='*76}")
        
        print(f"\n   Rule Violations:")
        for rule_name, count in metrics['failed_rules'].items():
            print(f"   - {rule_name}: {count} violations")
        
        # Step 5: Show cleaned data
        print("\n✅ Step 5: Cleaned data (valid records only):")
        cleaned_df.show(truncate=False)
        
        # Step 6: Show quarantined data
        print("\n⚠️  Step 6: Quarantined data (invalid records):")
        try:
            quarantine_df = spark.read.parquet("/tmp/quarantine/users")
            print(f"   Found {quarantine_df.count()} quarantined records")
            quarantine_df.select(
                "user_id", "email", "age", "country", "quality_score", "failed_rules"
            ).show(truncate=False)
        except:
            print("   No quarantined records")
        
        # Step 7: Generate quality report
        print("\n📈 Step 7: Generating quality report...")
        
        report = f"""
        DATA QUALITY REPORT
        Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        OVERALL STATISTICS:
        - Total Records Processed: {metrics['total_records']}
        - Valid Records: {metrics['valid_records']} ({metrics['quality_score']:.2f}%)
        - Invalid Records: {metrics['invalid_records']}
        - Data Quality Score: {metrics['quality_score']:.2f}%
        
        RULE VIOLATIONS:
        """
        
        for rule_name, count in sorted(metrics['failed_rules'].items(), 
                                      key=lambda x: x[1], reverse=True):
            report += f"        - {rule_name}: {count} violations\n"
        
        report += f"\n        Quality Status: {'PASS' if metrics['quality_score'] >= 80 else 'FAIL'}\n"
        
        print(report)
        
        # Step 8: Save cleaned data
        print("\n💾 Step 8: Saving cleaned data to Delta Lake...")
        
        final_df = cleaned_df.withColumn("quality_checked_at", current_timestamp())
        
        sink_config = SinkConfig(
            write_mode=WriteMode.OVERWRITE,
            partition_columns=['country'],
            enable_compression=True
        )
        
        sink = DeltaSink(spark, sink_config)
        sink.write(final_df, "/tmp/delta/users_cleaned")
        
        print("   ✅ Data saved successfully")
        
        print("\n" + "=" * 80)
        print("✨ Data Quality Pipeline Completed Successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()