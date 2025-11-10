"""
Streaming Pipeline Example

Demonstrates:
- Reading from Kafka
- Real-time transformations
- Writing to Delta Lake with streaming
- Checkpointing and monitoring
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from metaflow.sources.kafka_source import KafkaSource, KafkaSourceConfig
from metaflow.sinks.delta import DeltaSink, SinkConfig, WriteMode
from metaflow.runners.streaming_runner import StreamingRunner, RunnerConfig, TriggerConfig, TriggerType


def create_spark_session():
    """Create and configure Spark session for streaming."""
    return SparkSession.builder \
        .appName("MetaFlow-Streaming-Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()


def define_schema():
    """Define schema for incoming Kafka messages."""
    return StructType([
        StructField("user_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("page_url", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("country", StringType(), True)
    ])


def main():
    """Run streaming pipeline example."""
    print("=" * 80)
    print("MetaFlow Streaming Pipeline Example")
    print("=" * 80)
    
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        # Step 1: Configure Kafka source
        print("\n📥 Step 1: Configuring Kafka Source...")
        kafka_config = KafkaSourceConfig(
            bootstrap_servers="localhost:9092",
            topic="user_events",
            group_id="metaflow_consumer_group",
            starting_offsets="latest",
            max_offsets_per_trigger=10000,
            kafka_options={
                "failOnDataLoss": "false",
                "maxOffsetsPerTrigger": "10000"
            }
        )
        
        kafka_source = KafkaSource(spark, kafka_config)
        
        # Step 2: Read stream
        print("\n📊 Step 2: Reading Kafka stream...")
        raw_stream = kafka_source.read()
        
        # Step 3: Parse JSON and transform
        print("\n🔄 Step 3: Parsing and transforming data...")
        schema = define_schema()
        
        parsed_stream = raw_stream \
            .select(
                from_json(col("value").cast("string"), schema).alias("data"),
                col("timestamp").alias("kafka_timestamp"),
                col("topic"),
                col("partition"),
                col("offset")
            ) \
            .select(
                "data.*",
                "kafka_timestamp",
                "topic",
                "partition",
                "offset"
            )
        
        # Add processing timestamp
        enriched_stream = parsed_stream \
            .withColumn("processed_at", current_timestamp())
        
        # Step 4: Configure streaming runner
        print("\n⚙️  Step 4: Configuring streaming runner...")
        runner_config = RunnerConfig(
            checkpoint_location="/tmp/checkpoints/user_events",
            enable_checkpointing=True,
            enable_metrics=True,
            query_name="user_events_stream"
        )
        
        trigger_config = TriggerConfig(
            trigger_type=TriggerType.PROCESSING_TIME,
            interval="10 seconds"
        )
        
        # Step 5: Configure Delta sink
        print("\n💾 Step 5: Configuring Delta Lake sink...")
        sink_config = SinkConfig(
            write_mode=WriteMode.APPEND,
            partition_columns=['country', 'event_type'],
            enable_compression=True,
            enable_optimize=False,  # Don't optimize during streaming
            merge_schema=True
        )
        
        # Step 6: Start streaming query
        print("\n▶️  Step 6: Starting streaming query...")
        print("\n   Streaming is now active. Press Ctrl+C to stop.")
        print("   " + "-" * 76)
        
        query = enriched_stream.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", runner_config.checkpoint_location) \
            .trigger(processingTime=trigger_config.interval) \
            .partitionBy(*sink_config.partition_columns) \
            .start("/tmp/delta/user_events")
        
        # Monitor progress
        import time
        while query.isActive:
            progress = query.lastProgress
            if progress:
                print(f"\n   📊 Progress Report:")
                print(f"   - Batch ID: {progress.get('batchId', 'N/A')}")
                print(f"   - Input Rows: {progress.get('numInputRows', 0)}")
                print(f"   - Processing Rate: {progress.get('processedRowsPerSecond', 0):.2f} rows/sec")
                
                sources = progress.get('sources', [])
                if sources:
                    for source in sources:
                        print(f"   - Start Offset: {source.get('startOffset', 'N/A')}")
                        print(f"   - End Offset: {source.get('endOffset', 'N/A')}")
            
            time.sleep(10)
        
        # Wait for termination
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Stopping streaming pipeline...")
        print("=" * 80)
        print("✨ Streaming Pipeline Stopped Successfully!")
        print("=" * 80)
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()