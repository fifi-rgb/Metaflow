"""
Incremental Pipeline Example (CDC)

Demonstrates:
- Change Data Capture processing
- Incremental loading
- Merge/Upsert operations
- Watermarking
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, current_timestamp
from datetime import datetime

from metaflow.sources.jdbc_source import JdbcSource, SourceConfig
from metaflow.sinks.delta import DeltaSink, SinkConfig, WriteMode
from metaflow.runners.incremental_runner import IncrementalRunner, RunnerConfig, IncrementalStrategy


def create_spark_session():
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName("MetaFlow-Incremental-Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def get_last_watermark(spark, delta_path, watermark_column):
    """Get the last processed watermark value."""
    try:
        df = spark.read.format("delta").load(delta_path)
        last_value = df.agg(spark_max(watermark_column)).collect()[0][0]
        return last_value
    except:
        return None


def main():
    """Run incremental pipeline example."""
    print("=" * 80)
    print("MetaFlow Incremental Pipeline Example (CDC)")
    print("=" * 80)
    
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        # Configuration
        delta_path = "/tmp/delta/orders"
        watermark_column = "updated_at"
        primary_keys = ["order_id"]
        
        # Step 1: Get last watermark
        print("\n🔍 Step 1: Getting last watermark...")
        last_watermark = get_last_watermark(spark, delta_path, watermark_column)
        
        if last_watermark:
            print(f"   Last watermark: {last_watermark}")
        else:
            print("   No watermark found (initial load)")
            last_watermark = datetime(2020, 1, 1)
        
        # Step 2: Configure incremental source
        print("\n📥 Step 2: Configuring incremental source...")
        
        # Build incremental query
        query = f"""
        SELECT * FROM orders 
        WHERE {watermark_column} > '{last_watermark}'
        ORDER BY {watermark_column}
        """
        
        source_config = SourceConfig(
            connection_url="jdbc:postgresql://localhost:5432/sourcedb",
            query=query,
            user="postgres",
            password="password",
            driver="org.postgresql.Driver",
            fetch_size=10000
        )
        
        source = JdbcSource(spark, source_config)
        
        # Step 3: Read incremental data
        print("\n📊 Step 3: Reading incremental data...")
        incremental_df = source.read()
        record_count = incremental_df.count()
        
        if record_count == 0:
            print("   No new records to process")
            return
        
        print(f"   Found {record_count} new/updated records")
        
        # Step 4: Add processing metadata
        print("\n🏷️  Step 4: Adding metadata...")
        processed_df = incremental_df \
            .withColumn("processed_at", current_timestamp())
        
        # Show sample
        print("\n   Sample of incremental data:")
        processed_df.show(5, truncate=False)
        
        # Step 5: Configure runner for merge operation
        print("\n⚙️  Step 5: Configuring incremental runner...")
        runner_config = RunnerConfig(
            watermark_column=watermark_column,
            primary_keys=primary_keys,
            enable_checkpointing=True,
            checkpoint_location="/tmp/checkpoints/orders_incremental",
            enable_metrics=True
        )
        
        runner = IncrementalRunner(runner_config)
        
        # Step 6: Perform merge/upsert
        print("\n🔄 Step 6: Performing merge/upsert operation...")
        
        # Check if target exists
        try:
            target_df = spark.read.format("delta").load(delta_path)
            print(f"   Target table exists with {target_df.count()} records")
            
            # Perform merge
            from delta.tables import DeltaTable
            
            delta_table = DeltaTable.forPath(spark, delta_path)
            
            # Build merge condition
            merge_condition = " AND ".join([
                f"target.{key} = source.{key}" for key in primary_keys
            ])
            
            # Execute merge
            delta_table.alias("target").merge(
                processed_df.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            
            print(f"   ✅ Merge completed successfully")
            
        except:
            print(f"   Target table doesn't exist, performing initial load...")
            
            sink_config = SinkConfig(
                write_mode=WriteMode.OVERWRITE,
                partition_columns=['order_date'],
                enable_compression=True
            )
            
            sink = DeltaSink(spark, sink_config)
            sink.write(processed_df, delta_path)
        
        # Step 7: Get metrics
        print("\n📈 Step 7: Pipeline metrics...")
        final_df = spark.read.format("delta").load(delta_path)
        print(f"   Total records in target: {final_df.count()}")
        print(f"   New watermark: {processed_df.agg(spark_max(watermark_column)).collect()[0][0]}")
        
        # Step 8: Optimize table
        print("\n🔧 Step 8: Optimizing Delta table...")
        spark.sql(f"OPTIMIZE delta.`{delta_path}`")
        print("   ✅ Optimization completed")
        
        print("\n" + "=" * 80)
        print("✨ Incremental Pipeline Completed Successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()