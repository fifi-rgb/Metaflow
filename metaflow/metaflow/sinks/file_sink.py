"""
File sink for writing to various file formats.

Supports:
- Parquet, CSV, JSON, Avro, ORC
- Partitioned writes
- Compression
- Schema evolution
"""

from typing import Dict, Any, Optional
from pyspark.sql import DataFrame

from metaflow.sinks.base_sink import BaseSink, SinkConfig, WriteMode


class FileSink(BaseSink):
    """
    Sink for writing data to file systems.
    
    Supports:
    - Multiple file formats (Parquet, CSV, JSON, Avro, ORC)
    - Partitioned writes
    - Compression codecs
    - Schema evolution
    """
    
    def __init__(
        self,
        spark,
        config: SinkConfig,
        output_path: str,
        file_format: str = 'parquet'
    ):
        """
        Initialize file sink.
        
        Args:
            spark: Active SparkSession
            config: Sink configuration
            output_path: Output file path
            file_format: File format (parquet, csv, json, avro, orc)
        """
        super().__init__(spark, config)
        self.output_path = output_path
        self.file_format = file_format.lower()
    
    def _do_write(self, df: DataFrame, **kwargs) -> None:
        """
        Write DataFrame to files.
        
        Args:
            df: DataFrame to write
            **kwargs: Additional format-specific options
        """
        writer = df.write.format(self.file_format).mode(self.config.write_mode.value)
        
        # Apply partitioning
        if self.config.partition_columns:
            writer = writer.partitionBy(*self.config.partition_columns)
        
        # Apply compression
        if self.config.enable_compression:
            writer = writer.option('compression', self.config.compression_codec)
        
        # Apply bucketing if configured
        if self.config.bucketing_columns and self.config.num_buckets > 0:
            writer = writer.bucketBy(
                self.config.num_buckets,
                *self.config.bucketing_columns
            )
        
        # Format-specific options
        if self.file_format == 'csv':
            writer = self._apply_csv_options(writer, kwargs)
        elif self.file_format == 'json':
            writer = self._apply_json_options(writer, kwargs)
        elif self.file_format == 'parquet':
            writer = self._apply_parquet_options(writer, kwargs)
        elif self.file_format == 'avro':
            writer = self._apply_avro_options(writer, kwargs)
        
        # Apply custom options
        for key, value in self.config.custom_options.items():
            writer = writer.option(key, value)
        
        # Control file size
        if self.config.max_records_per_file > 0:
            writer = writer.option('maxRecordsPerFile', str(self.config.max_records_per_file))
        
        writer.save(self.output_path)
        
        self.logger.info(f"Wrote {self.file_format} files to: {self.output_path}")
    
    def _apply_csv_options(self, writer, kwargs: Dict[str, Any]):
        """Apply CSV-specific options."""
        writer = writer.option('header', str(kwargs.get('header', True)))
        writer = writer.option('delimiter', kwargs.get('delimiter', ','))
        writer = writer.option('quote', kwargs.get('quote', '"'))
        writer = writer.option('escape', kwargs.get('escape', '\\'))
        writer = writer.option('nullValue', kwargs.get('nullValue', ''))
        writer = writer.option('dateFormat', kwargs.get('dateFormat', 'yyyy-MM-dd'))
        writer = writer.option('timestampFormat', kwargs.get('timestampFormat', 'yyyy-MM-dd HH:mm:ss'))
        
        return writer
    
    def _apply_json_options(self, writer, kwargs: Dict[str, Any]):
        """Apply JSON-specific options."""
        writer = writer.option('dateFormat', kwargs.get('dateFormat', 'yyyy-MM-dd'))
        writer = writer.option('timestampFormat', kwargs.get('timestampFormat', 'yyyy-MM-dd HH:mm:ss'))
        writer = writer.option('multiLine', str(kwargs.get('multiLine', False)))
        
        return writer
    
    def _apply_parquet_options(self, writer, kwargs: Dict[str, Any]):
        """Apply Parquet-specific options."""
        writer = writer.option('mergeSchema', str(self.config.merge_schema))
        
        return writer
    
    def _apply_avro_options(self, writer, kwargs: Dict[str, Any]):
        """Apply Avro-specific options."""
        if 'avroSchema' in kwargs:
            writer = writer.option('avroSchema', kwargs['avroSchema'])
        
        return writer
    
    def list_output_files(self) -> list:
        """
        List files written to output path.
        
        Returns:
            List of file paths
        """
        from pathlib import Path
        
        output_path = Path(self.output_path)
        
        if output_path.exists():
            return [str(f) for f in output_path.rglob(f'*.{self.file_format}')]
        
        return []