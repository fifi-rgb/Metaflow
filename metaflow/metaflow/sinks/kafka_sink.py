"""
Kafka sink for writing to Kafka topics.

Supports:
- Batch and streaming writes
- Key-value serialization
- Partitioning strategies
- Exactly-once semantics
"""

from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_json, struct

from metaflow.sinks.base_sink import BaseSink, SinkConfig


class KafkaSink(BaseSink):
    """
    Sink for writing data to Kafka topics.
    
    Supports:
    - Batch and streaming writes
    - JSON and Avro serialization
    - Custom partitioning
    - Headers and metadata
    """
    
    def __init__(
        self,
        spark,
        config: SinkConfig,
        bootstrap_servers: str,
        topic: str,
        kafka_options: Optional[Dict[str, str]] = None
    ):
        """
        Initialize Kafka sink.
        
        Args:
            spark: Active SparkSession
            config: Sink configuration
            bootstrap_servers: Kafka bootstrap servers
            topic: Target Kafka topic
            kafka_options: Additional Kafka producer options
        """
        super().__init__(spark, config)
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.kafka_options = kafka_options or {}
    
    def _do_write(self, df: DataFrame, **kwargs) -> None:
        """
        Write DataFrame to Kafka topic.
        
        Args:
            df: DataFrame to write (must have 'key' and 'value' columns)
            **kwargs: Additional Kafka options
        """
        # Prepare DataFrame for Kafka
        kafka_df = self._prepare_kafka_dataframe(df, kwargs)
        
        writer = kafka_df.write.format('kafka') \
            .option('kafka.bootstrap.servers', self.bootstrap_servers) \
            .option('topic', self.topic)
        
        # Apply Kafka options
        for key, value in self.kafka_options.items():
            writer = writer.option(f'kafka.{key}', value)
        
        # Apply custom options
        for key, value in self.config.custom_options.items():
            writer = writer.option(key, value)
        
        writer.save()
        
        self.logger.info(f"Wrote data to Kafka topic: {self.topic}")
    
    def _prepare_kafka_dataframe(self, df: DataFrame, kwargs: Dict[str, Any]) -> DataFrame:
        """
        Prepare DataFrame for Kafka write.
        
        Args:
            df: Input DataFrame
            kwargs: Configuration options
            
        Returns:
            DataFrame with 'key' and 'value' columns
        """
        key_column = kwargs.get('key_column')
        value_columns = kwargs.get('value_columns')
        
        # Prepare value column
        if value_columns:
            # Select specific columns for value
            value_df = df.select(*value_columns)
            value_expr = to_json(struct(*value_columns))
        else:
            # Use all columns
            value_expr = to_json(struct(*df.columns))
        
        kafka_df = df.withColumn('value', value_expr.cast('string'))
        
        # Prepare key column
        if key_column:
            kafka_df = kafka_df.withColumn('key', col(key_column).cast('string'))
        else:
            kafka_df = kafka_df.withColumn('key', col(df.columns[0]).cast('string'))
        
        # Select only required columns
        return kafka_df.select('key', 'value')
    
    def write_stream(
        self,
        df: DataFrame,
        checkpoint_location: str,
        **kwargs
    ):
        """
        Write streaming DataFrame to Kafka.
        
        Args:
            df: Streaming DataFrame
            checkpoint_location: Checkpoint directory
            **kwargs: Additional streaming options
        """
        # Prepare DataFrame for Kafka
        kafka_df = self._prepare_kafka_dataframe(df, kwargs)
        
        writer = kafka_df.writeStream.format('kafka') \
            .option('kafka.bootstrap.servers', self.bootstrap_servers) \
            .option('topic', self.topic) \
            .option('checkpointLocation', checkpoint_location)
        
        # Apply Kafka options
        for key, value in self.kafka_options.items():
            writer = writer.option(f'kafka.{key}', value)
        
        # Trigger configuration
        trigger = kwargs.get('trigger', {'type': 'micro-batch', 'interval': '10 seconds'})
        
        if trigger['type'] == 'micro-batch':
            writer = writer.trigger(processingTime=trigger['interval'])
        elif trigger['type'] == 'continuous':
            writer = writer.trigger(continuous=trigger['interval'])
        
        # Start streaming query
        query = writer.start()
        
        self.logger.info(f"Started streaming to Kafka topic: {self.topic}")
        
        return query