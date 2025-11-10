"""
Streaming runner for real-time data pipelines.

Supports:
- Kafka streaming sources
- Real-time transformations
- Micro-batch processing
- Exactly-once semantics
"""

from typing import Dict, Any, Optional
from datetime import datetime
import time

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import StructType

from metaflow.runners.base_runner import BaseRunner, RunnerConfig, PipelineMetrics


class StreamingRunner(BaseRunner):
    """
    Runner for streaming data pipeline execution.
    
    Features:
    - Kafka and file streaming sources
    - Micro-batch and continuous processing
    - Exactly-once semantics
    - Watermark-based event time processing
    - State management
    """
    
    def __init__(self, config: RunnerConfig):
        """
        Initialize streaming runner.
        
        Args:
            config: Runner configuration
        """
        super().__init__(config)
        self._active_queries: list[StreamingQuery] = []
    
    def run(self, pipeline_config: Dict[str, Any]) -> PipelineMetrics:
        """
        Execute streaming pipeline.
        
        Args:
            pipeline_config: Pipeline configuration containing:
                - pipeline_id: Unique identifier
                - source: Streaming source configuration
                - target: Target Delta table path
                - trigger: Trigger configuration (default: micro-batch)
                - watermark: Event time watermark settings
                
        Returns:
            Pipeline execution metrics
        """
        self.validate_config(pipeline_config)
        
        pipeline_id = pipeline_config['pipeline_id']
        self.logger.info(f"Starting streaming pipeline: {pipeline_id}")
        
        # Initialize
        self.spark = self._initialize_spark()
        self.metrics = self._initialize_metrics(pipeline_id)
        
        try:
            # Create streaming source
            stream_df = self._create_stream(pipeline_config)
            
            # Transform stream
            transformed_df = self._transform_stream(stream_df, pipeline_config)
            
            # Start streaming query
            query = self._start_streaming_query(transformed_df, pipeline_config)
            self._active_queries.append(query)
            
            # Monitor until completion or interruption
            self._monitor_streaming_query(query, pipeline_config)
            
            self._finalize_metrics(success=True)
            
        except Exception as e:
            self.logger.error(f"Streaming pipeline failed: {e}", exc_info=True)
            self._finalize_metrics(success=False, error=e)
            raise
        
        finally:
            self._stop_all_queries()
            self._trigger_callbacks()
            self.cleanup()
        
        return self.metrics
    
    def _create_stream(self, config: Dict[str, Any]) -> DataFrame:
        """
        Create streaming DataFrame from source.
        
        Args:
            config: Pipeline configuration
            
        Returns:
            Streaming DataFrame
        """
        source_config = config['source']
        source_type = source_config.get('type', 'kafka')
        
        self.logger.info(f"Creating {source_type} stream...")
        
        if source_type == 'kafka':
            return self._create_kafka_stream(source_config)
        elif source_type == 'file':
            return self._create_file_stream(source_config)
        else:
            raise ValueError(f"Unsupported streaming source: {source_type}")
    
    def _create_kafka_stream(self, source_config: Dict[str, Any]) -> DataFrame:
        """Create Kafka streaming source."""
        kafka_options = {
            'kafka.bootstrap.servers': source_config['bootstrap_servers'],
            'subscribe': source_config['topic'],
            'startingOffsets': source_config.get('starting_offsets', 'latest'),
            'failOnDataLoss': source_config.get('fail_on_data_loss', 'false'),
        }
        
        # Add authentication if provided
        if 'security_protocol' in source_config:
            kafka_options['kafka.security.protocol'] = source_config['security_protocol']
            
        if 'sasl_mechanism' in source_config:
            kafka_options['kafka.sasl.mechanism'] = source_config['sasl_mechanism']
            kafka_options['kafka.sasl.jaas.config'] = source_config['sasl_jaas_config']
        
        stream_df = self.spark.readStream.format('kafka').options(**kafka_options).load()
        
        # Parse JSON if schema provided
        if 'schema' in source_config:
            schema = self._parse_schema(source_config['schema'])
            stream_df = stream_df.select(
                from_json(col('value').cast('string'), schema).alias('data'),
                col('timestamp').alias('kafka_timestamp'),
                col('partition'),
                col('offset')
            ).select('data.*', 'kafka_timestamp', 'partition', 'offset')
        
        return stream_df
    
    def _create_file_stream(self, source_config: Dict[str, Any]) -> DataFrame:
        """Create file streaming source."""
        file_path = source_config['path']
        file_format = source_config.get('format', 'json')
        
        reader = self.spark.readStream.format(file_format)
        
        if 'schema' in source_config:
            schema = self._parse_schema(source_config['schema'])
            reader = reader.schema(schema)
        
        return reader.load(file_path)
    
    def _parse_schema(self, schema_config: Any) -> StructType:
        """
        Parse schema configuration.
        
        Args:
            schema_config: Schema as dict or StructType
            
        Returns:
            StructType schema
        """
        if isinstance(schema_config, StructType):
            return schema_config
        
        # Convert dict to StructType
        from pyspark.sql.types import StructField
        
        fields = []
        for field_name, field_type in schema_config.items():
            # Simple type mapping
            spark_type = self._get_spark_type(field_type)
            fields.append(StructField(field_name, spark_type, True))
        
        return StructType(fields)
    
    def _get_spark_type(self, type_str: str):
        """Map type string to Spark type."""
        from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, BooleanType, TimestampType
        
        type_mapping = {
            'string': StringType(),
            'int': IntegerType(),
            'long': LongType(),
            'double': DoubleType(),
            'boolean': BooleanType(),
            'timestamp': TimestampType(),
        }
        
        return type_mapping.get(type_str.lower(), StringType())
    
    def _transform_stream(self, stream_df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        """
        Apply transformations to stream.
        
        Args:
            stream_df: Streaming DataFrame
            config: Pipeline configuration
            
        Returns:
            Transformed streaming DataFrame
        """
        # Add metadata
        stream_df = stream_df.withColumn('_metaflow_timestamp', current_timestamp())
        stream_df = stream_df.withColumn('_metaflow_pipeline_id', lit(config['pipeline_id']))
        
        # Apply watermark if specified
        if 'watermark' in config:
            watermark_config = config['watermark']
            stream_df = stream_df.withWatermark(
                watermark_config['column'],
                watermark_config['threshold']
            )
        
        # Apply custom transformations
        if 'transformations' in config:
            for transform in config['transformations']:
                transform_type = transform['type']
                
                if transform_type == 'filter':
                    stream_df = stream_df.filter(transform['condition'])
                elif transform_type == 'select':
                    stream_df = stream_df.select(*transform['columns'])
                elif transform_type == 'aggregate':
                    # Window aggregations
                    stream_df = self._apply_aggregation(stream_df, transform)
        
        return stream_df
    
    def _apply_aggregation(self, df: DataFrame, transform: Dict[str, Any]) -> DataFrame:
        """Apply windowed aggregation."""
        from pyspark.sql.functions import window
        
        if 'window' in transform:
            window_config = transform['window']
            df = df.groupBy(
                window(col(window_config['column']), window_config['duration']),
                *transform.get('group_by', [])
            ).agg(*transform['aggregations'])
        
        return df
    
    def _start_streaming_query(self, stream_df: DataFrame, config: Dict[str, Any]) -> StreamingQuery:
        """
        Start streaming query.
        
        Args:
            stream_df: Streaming DataFrame
            config: Pipeline configuration
            
        Returns:
            Active StreamingQuery
        """
        target_path = config['target']
        checkpoint_path = str(self._create_checkpoint_dir(config['pipeline_id']))
        
        writer = stream_df.writeStream \
            .format('delta') \
            .outputMode(config.get('output_mode', 'append')) \
            .option('checkpointLocation', checkpoint_path)
        
        # Configure trigger
        trigger_config = config.get('trigger', {'type': 'micro-batch', 'interval': '10 seconds'})
        
        if trigger_config['type'] == 'micro-batch':
            writer = writer.trigger(processingTime=trigger_config['interval'])
        elif trigger_config['type'] == 'continuous':
            writer = writer.trigger(continuous=trigger_config['interval'])
        elif trigger_config['type'] == 'once':
            writer = writer.trigger(once=True)
        
        # Start query
        query = writer.start(target_path)
        
        self.logger.info(f"Streaming query started: {query.id}")
        return query
    
    def _monitor_streaming_query(self, query: StreamingQuery, config: Dict[str, Any]) -> None:
        """
        Monitor streaming query progress.
        
        Args:
            query: Active streaming query
            config: Pipeline configuration
        """
        max_runtime = config.get('max_runtime_seconds', None)
        start_time = time.time()
        
        try:
            while query.isActive:
                progress = query.lastProgress
                
                if progress:
                    self.logger.info(
                        f"Batch {progress['batchId']}: "
                        f"{progress['numInputRows']} rows processed, "
                        f"rate: {progress.get('processedRowsPerSecond', 0):.2f} rows/sec"
                    )
                    
                    # Update metrics
                    self.metrics.records_read += progress['numInputRows']
                    
                # Check max runtime
                if max_runtime and (time.time() - start_time) > max_runtime:
                    self.logger.info("Max runtime reached, stopping query...")
                    query.stop()
                    break
                
                time.sleep(5)
        
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, stopping query...")
            query.stop()
    
    def _stop_all_queries(self) -> None:
        """Stop all active streaming queries."""
        for query in self._active_queries:
            if query.isActive:
                self.logger.info(f"Stopping query: {query.id}")
                query.stop()
        
        self._active_queries.clear()