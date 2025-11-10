"""
Base sink class for all MetaFlow data sinks.

Provides common functionality for data writing, error handling,
metrics collection, and validation.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional, List, Callable

from pyspark.sql import DataFrame, SparkSession


class WriteMode(Enum):
    """Data write modes."""
    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"
    ERROR_IF_EXISTS = "error"
    IGNORE = "ignore"


@dataclass
class SinkConfig:
    """Configuration for data sinks."""
    
    # Write settings
    write_mode: WriteMode = WriteMode.APPEND
    batch_size: int = 10000
    max_records_per_file: int = 1000000
    
    # Partitioning
    partition_columns: List[str] = field(default_factory=list)
    bucketing_columns: List[str] = field(default_factory=list)
    num_buckets: int = 0
    
    # Optimization
    coalesce_partitions: bool = True
    target_partition_size_mb: int = 128
    enable_compression: bool = True
    compression_codec: str = "snappy"
    
    # Schema management
    enable_schema_evolution: bool = True
    enable_schema_validation: bool = True
    merge_schema: bool = False
    
    # Error handling
    error_handling_mode: str = "fail"  # fail, skip, quarantine
    quarantine_path: Optional[str] = None
    max_errors_threshold: int = 100
    
    # Retry settings
    max_retries: int = 3
    retry_delay_seconds: int = 30
    exponential_backoff: bool = True
    
    # Monitoring
    enable_metrics: bool = True
    enable_write_validation: bool = True
    sample_validation_rate: float = 0.01
    
    # Custom configurations
    custom_options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SinkMetrics:
    """Metrics collected during sink operations."""
    
    sink_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    
    # Record counts
    records_attempted: int = 0
    records_written: int = 0
    records_failed: int = 0
    records_quarantined: int = 0
    
    # Performance
    duration_seconds: float = 0.0
    throughput_records_per_second: float = 0.0
    data_size_mb: float = 0.0
    
    # Files/partitions
    files_written: int = 0
    partitions_written: int = 0
    
    # Errors
    error_count: int = 0
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            'sink_name': self.sink_name,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'records_attempted': self.records_attempted,
            'records_written': self.records_written,
            'records_failed': self.records_failed,
            'records_quarantined': self.records_quarantined,
            'duration_seconds': self.duration_seconds,
            'throughput_records_per_second': self.throughput_records_per_second,
            'data_size_mb': self.data_size_mb,
            'files_written': self.files_written,
            'partitions_written': self.partitions_written,
            'error_count': self.error_count,
            'error_message': self.error_message,
        }


class BaseSink(ABC):
    """
    Abstract base class for all data sinks.
    
    Provides common functionality for:
    - Data writing with retries
    - Schema validation
    - Error handling and quarantine
    - Metrics collection
    - Pre/post write hooks
    """
    
    def __init__(self, spark: SparkSession, config: SinkConfig):
        """
        Initialize the sink.
        
        Args:
            spark: Active SparkSession
            config: Sink configuration
        """
        self.spark = spark
        self.config = config
        self.logger = self._setup_logging()
        self.metrics: Optional[SinkMetrics] = None
        self._pre_write_hooks: List[Callable] = []
        self._post_write_hooks: List[Callable] = []
        
    def _setup_logging(self) -> logging.Logger:
        """Configure logging for the sink."""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def write(self, df: DataFrame, **kwargs) -> SinkMetrics:
        """
        Write DataFrame to sink with retries and error handling.
        
        Args:
            df: DataFrame to write
            **kwargs: Additional write options
            
        Returns:
            Metrics from write operation
        """
        self.metrics = SinkMetrics(
            sink_name=self.__class__.__name__,
            start_time=datetime.now()
        )
        
        self.metrics.records_attempted = df.count()
        self.logger.info(f"Writing {self.metrics.records_attempted} records to {self.__class__.__name__}")
        
        try:
            # Pre-write hooks
            df = self._execute_pre_write_hooks(df)
            
            # Validate schema
            if self.config.enable_schema_validation:
                self._validate_schema(df)
            
            # Optimize DataFrame
            df = self._optimize_dataframe(df)
            
            # Write with retries
            self._write_with_retry(df, **kwargs)
            
            # Post-write validation
            if self.config.enable_write_validation:
                self._validate_write(df)
            
            # Post-write hooks
            self._execute_post_write_hooks(df)
            
            self.metrics.records_written = self.metrics.records_attempted - self.metrics.records_failed
            
        except Exception as e:
            self.logger.error(f"Write failed: {e}", exc_info=True)
            self.metrics.error_message = str(e)
            self.metrics.error_count += 1
            raise
        
        finally:
            self._finalize_metrics()
        
        return self.metrics
    
    def _write_with_retry(self, df: DataFrame, **kwargs) -> None:
        """
        Write data with retry logic.
        
        Args:
            df: DataFrame to write
            **kwargs: Additional write options
        """
        import time
        
        last_exception = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                self._do_write(df, **kwargs)
                return
                
            except Exception as e:
                last_exception = e
                
                if attempt < self.config.max_retries:
                    delay = self._calculate_retry_delay(attempt)
                    self.logger.warning(
                        f"Write attempt {attempt + 1} failed, retrying in {delay}s: {e}"
                    )
                    time.sleep(delay)
                else:
                    self.logger.error(f"Write failed after {self.config.max_retries + 1} attempts")
        
        if last_exception:
            raise last_exception
    
    def _calculate_retry_delay(self, attempt: int) -> float:
        """
        Calculate retry delay with optional exponential backoff.
        
        Args:
            attempt: Retry attempt number
            
        Returns:
            Delay in seconds
        """
        if self.config.exponential_backoff:
            return self.config.retry_delay_seconds * (2 ** attempt)
        return self.config.retry_delay_seconds
    
    @abstractmethod
    def _do_write(self, df: DataFrame, **kwargs) -> None:
        """
        Perform the actual write operation.
        
        Subclasses must implement this method.
        
        Args:
            df: DataFrame to write
            **kwargs: Additional write options
        """
        pass
    
    def _optimize_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Optimize DataFrame before writing.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Optimized DataFrame
        """
        # Coalesce partitions if needed
        if self.config.coalesce_partitions:
            current_partitions = df.rdd.getNumPartitions()
            target_partitions = self._calculate_target_partitions(df)
            
            if current_partitions > target_partitions:
                self.logger.info(f"Coalescing from {current_partitions} to {target_partitions} partitions")
                df = df.coalesce(target_partitions)
        
        # Cache if beneficial
        if df.storageLevel.useMemory or df.storageLevel.useDisk:
            df = df.cache()
        
        return df
    
    def _calculate_target_partitions(self, df: DataFrame) -> int:
        """
        Calculate optimal number of partitions.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Target partition count
        """
        # Estimate data size
        from pyspark.sql import Row
        
        try:
            sample = df.limit(1000).collect()
            avg_row_size = sum(len(str(row)) for row in sample) / len(sample) if sample else 100
            total_size_mb = (df.count() * avg_row_size) / (1024 * 1024)
            
            target_partitions = max(1, int(total_size_mb / self.config.target_partition_size_mb))
            return target_partitions
            
        except Exception as e:
            self.logger.warning(f"Could not calculate target partitions: {e}")
            return df.rdd.getNumPartitions()
    
    def _validate_schema(self, df: DataFrame) -> None:
        """
        Validate DataFrame schema.
        
        Args:
            df: DataFrame to validate
        """
        # Check for null column names
        for field in df.schema.fields:
            if not field.name or field.name.strip() == '':
                raise ValueError("DataFrame contains columns with empty names")
        
        # Check for duplicate column names
        column_names = [field.name for field in df.schema.fields]
        if len(column_names) != len(set(column_names)):
            duplicates = [name for name in column_names if column_names.count(name) > 1]
            raise ValueError(f"DataFrame contains duplicate column names: {duplicates}")
    
    def _validate_write(self, df: DataFrame) -> None:
        """
        Validate write operation by sampling data.
        
        Args:
            df: Original DataFrame
        """
        # Default implementation - can be overridden by subclasses
        self.logger.debug("Write validation completed")
    
    def _execute_pre_write_hooks(self, df: DataFrame) -> DataFrame:
        """
        Execute registered pre-write hooks.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        for hook in self._pre_write_hooks:
            try:
                df = hook(df)
            except Exception as e:
                self.logger.error(f"Pre-write hook failed: {e}")
                raise
        
        return df
    
    def _execute_post_write_hooks(self, df: DataFrame) -> None:
        """
        Execute registered post-write hooks.
        
        Args:
            df: Written DataFrame
        """
        for hook in self._post_write_hooks:
            try:
                hook(df)
            except Exception as e:
                self.logger.error(f"Post-write hook failed: {e}")
    
    def register_pre_write_hook(self, hook: Callable[[DataFrame], DataFrame]) -> None:
        """
        Register hook to execute before writing.
        
        Args:
            hook: Function that takes and returns DataFrame
        """
        self._pre_write_hooks.append(hook)
    
    def register_post_write_hook(self, hook: Callable[[DataFrame], None]) -> None:
        """
        Register hook to execute after writing.
        
        Args:
            hook: Function that takes DataFrame
        """
        self._post_write_hooks.append(hook)
    
    def _finalize_metrics(self) -> None:
        """Finalize metrics after write completion."""
        if not self.metrics:
            return
        
        self.metrics.end_time = datetime.now()
        self.metrics.duration_seconds = (
            self.metrics.end_time - self.metrics.start_time
        ).total_seconds()
        
        if self.metrics.records_written > 0 and self.metrics.duration_seconds > 0:
            self.metrics.throughput_records_per_second = (
                self.metrics.records_written / self.metrics.duration_seconds
            )
        
        self.logger.info(f"Write metrics: {self.metrics.to_dict()}")
    
    def _handle_write_error(self, df: DataFrame, error: Exception) -> None:
        """
        Handle write errors based on configuration.
        
        Args:
            df: DataFrame that failed to write
            error: Exception that occurred
        """
        if self.config.error_handling_mode == "fail":
            raise error
        
        elif self.config.error_handling_mode == "skip":
            self.logger.warning(f"Skipping failed records: {error}")
            self.metrics.records_failed += df.count()
        
        elif self.config.error_handling_mode == "quarantine" and self.config.quarantine_path:
            self.logger.warning(f"Quarantining failed records: {error}")
            self._quarantine_records(df, error)
        
        else:
            raise ValueError(f"Invalid error handling mode: {self.config.error_handling_mode}")
    
    def _quarantine_records(self, df: DataFrame, error: Exception) -> None:
        """
        Write failed records to quarantine location.
        
        Args:
            df: DataFrame with failed records
            error: Error that occurred
        """
        if not self.config.quarantine_path:
            return
        
        from pyspark.sql.functions import lit, current_timestamp
        
        quarantine_df = df.withColumn('_error_message', lit(str(error))) \
                          .withColumn('_quarantine_timestamp', current_timestamp())
        
        try:
            quarantine_df.write \
                .mode('append') \
                .parquet(self.config.quarantine_path)
            
            self.metrics.records_quarantined += df.count()
            self.logger.info(f"Quarantined {df.count()} records to {self.config.quarantine_path}")
            
        except Exception as qe:
            self.logger.error(f"Failed to quarantine records: {qe}")