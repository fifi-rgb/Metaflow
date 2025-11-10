"""
Base runner class for all MetaFlow pipeline executors.

Provides common functionality for pipeline execution, monitoring,
error handling, and checkpoint management.
"""

import time
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable

from pyspark.sql import SparkSession, DataFrame


class PipelineStatus(Enum):
    """Pipeline execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class RunnerConfig:
    """Configuration for pipeline runners."""
    
    # Execution settings
    spark_master: str = "local[*]"
    app_name: str = "MetaFlow-Pipeline"
    checkpoint_location: str = "/tmp/metaflow/checkpoints"
    log_level: str = "INFO"
    
    # Performance tuning
    shuffle_partitions: int = 200
    max_concurrent_tasks: int = 4
    memory_fraction: float = 0.8
    
    # Retry and fault tolerance
    max_retries: int = 3
    retry_delay_seconds: int = 60
    enable_checkpointing: bool = True
    checkpoint_interval_seconds: int = 300
    
    # Monitoring
    enable_metrics: bool = True
    metrics_port: int = 9091
    enable_progress_tracking: bool = True
    
    # Data quality
    enable_validation: bool = True
    validation_sample_rate: float = 0.1
    fail_on_validation_error: bool = False
    
    # Delta Lake settings
    delta_optimize_interval: int = 10  # Runs
    delta_vacuum_retention_hours: int = 168  # 7 days
    
    # Custom configurations
    custom_configs: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineMetrics:
    """Metrics collected during pipeline execution."""
    
    pipeline_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: PipelineStatus = PipelineStatus.PENDING
    
    # Record counts
    records_read: int = 0
    records_written: int = 0
    records_filtered: int = 0
    records_failed: int = 0
    
    # Performance metrics
    duration_seconds: float = 0.0
    throughput_records_per_second: float = 0.0
    
    # Resource usage
    peak_memory_mb: float = 0.0
    cpu_time_seconds: float = 0.0
    
    # Data quality
    validation_errors: int = 0
    duplicate_records: int = 0
    
    # Error information
    error_message: Optional[str] = None
    error_stacktrace: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            'pipeline_id': self.pipeline_id,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'status': self.status.value,
            'records_read': self.records_read,
            'records_written': self.records_written,
            'records_filtered': self.records_filtered,
            'records_failed': self.records_failed,
            'duration_seconds': self.duration_seconds,
            'throughput_records_per_second': self.throughput_records_per_second,
            'peak_memory_mb': self.peak_memory_mb,
            'cpu_time_seconds': self.cpu_time_seconds,
            'validation_errors': self.validation_errors,
            'duplicate_records': self.duplicate_records,
            'error_message': self.error_message,
        }


class BaseRunner(ABC):
    """
    Abstract base class for all pipeline runners.
    
    Provides common functionality for:
    - Spark session management
    - Checkpoint management
    - Metrics collection
    - Error handling and retry logic
    - Progress tracking
    """
    
    def __init__(self, config: RunnerConfig):
        """
        Initialize the runner.
        
        Args:
            config: Runner configuration
        """
        self.config = config
        self.logger = self._setup_logging()
        self.spark: Optional[SparkSession] = None
        self.metrics: Optional[PipelineMetrics] = None
        self._callbacks: List[Callable] = []
        
    def _setup_logging(self) -> logging.Logger:
        """Configure logging for the runner."""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(getattr(logging, self.config.log_level))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _initialize_spark(self) -> SparkSession:
        """
        Initialize Spark session with optimized configurations.
        
        Returns:
            Configured SparkSession
        """
        self.logger.info("Initializing Spark session...")
        
        builder = SparkSession.builder \
            .appName(self.config.app_name) \
            .master(self.config.spark_master)
        
        # Core configurations
        configs = {
            "spark.sql.shuffle.partitions": str(self.config.shuffle_partitions),
            "spark.memory.fraction": str(self.config.memory_fraction),
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            
            # Delta Lake configurations
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.delta.retentionDurationCheck.enabled": "false",
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
            
            # Performance tuning
            "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
            "spark.sql.autoBroadcastJoinThreshold": "10485760",  # 10MB
            "spark.default.parallelism": str(self.config.max_concurrent_tasks * 2),
        }
        
        # Add custom configurations
        configs.update(self.config.custom_configs)
        
        # Apply all configurations
        for key, value in configs.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(self.config.log_level)
        
        self.logger.info(f"Spark session initialized: {spark.version}")
        return spark
    
    def _create_checkpoint_dir(self, pipeline_id: str) -> Path:
        """
        Create checkpoint directory for pipeline.
        
        Args:
            pipeline_id: Unique pipeline identifier
            
        Returns:
            Path to checkpoint directory
        """
        checkpoint_path = Path(self.config.checkpoint_location) / pipeline_id
        checkpoint_path.mkdir(parents=True, exist_ok=True)
        return checkpoint_path
    
    def _save_checkpoint(self, pipeline_id: str, state: Dict[str, Any]) -> None:
        """
        Save pipeline checkpoint.
        
        Args:
            pipeline_id: Pipeline identifier
            state: State to checkpoint
        """
        if not self.config.enable_checkpointing:
            return
        
        checkpoint_path = self._create_checkpoint_dir(pipeline_id)
        checkpoint_file = checkpoint_path / "state.json"
        
        import json
        with open(checkpoint_file, 'w') as f:
            json.dump(state, f, indent=2, default=str)
        
        self.logger.debug(f"Checkpoint saved: {checkpoint_file}")
    
    def _load_checkpoint(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """
        Load pipeline checkpoint.
        
        Args:
            pipeline_id: Pipeline identifier
            
        Returns:
            Checkpoint state or None
        """
        if not self.config.enable_checkpointing:
            return None
        
        checkpoint_file = Path(self.config.checkpoint_location) / pipeline_id / "state.json"
        
        if not checkpoint_file.exists():
            return None
        
        import json
        with open(checkpoint_file, 'r') as f:
            state = json.load(f)
        
        self.logger.info(f"Checkpoint loaded: {checkpoint_file}")
        return state
    
    def _initialize_metrics(self, pipeline_id: str) -> PipelineMetrics:
        """
        Initialize metrics tracking.
        
        Args:
            pipeline_id: Pipeline identifier
            
        Returns:
            PipelineMetrics object
        """
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            start_time=datetime.now(),
            status=PipelineStatus.RUNNING
        )
    
    def _finalize_metrics(self, success: bool = True, error: Optional[Exception] = None) -> None:
        """
        Finalize metrics after pipeline completion.
        
        Args:
            success: Whether pipeline succeeded
            error: Exception if pipeline failed
        """
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
        
        if success:
            self.metrics.status = PipelineStatus.SUCCESS
        else:
            self.metrics.status = PipelineStatus.FAILED
            if error:
                self.metrics.error_message = str(error)
                import traceback
                self.metrics.error_stacktrace = traceback.format_exc()
        
        self.logger.info(f"Pipeline metrics: {self.metrics.to_dict()}")
    
    def register_callback(self, callback: Callable[[PipelineMetrics], None]) -> None:
        """
        Register callback to be called on pipeline completion.
        
        Args:
            callback: Function to call with metrics
        """
        self._callbacks.append(callback)
    
    def _trigger_callbacks(self) -> None:
        """Execute all registered callbacks."""
        if not self.metrics:
            return
        
        for callback in self._callbacks:
            try:
                callback(self.metrics)
            except Exception as e:
                self.logger.error(f"Callback failed: {e}")
    
    @abstractmethod
    def run(self, pipeline_config: Dict[str, Any]) -> PipelineMetrics:
        """
        Execute the pipeline.
        
        Args:
            pipeline_config: Pipeline-specific configuration
            
        Returns:
            Pipeline execution metrics
        """
        pass
    
    def validate_config(self, pipeline_config: Dict[str, Any]) -> bool:
        """
        Validate pipeline configuration.
        
        Args:
            pipeline_config: Configuration to validate
            
        Returns:
            True if valid, raises ValueError otherwise
        """
        required_fields = ['pipeline_id', 'source', 'target']
        
        for field in required_fields:
            if field not in pipeline_config:
                raise ValueError(f"Missing required field: {field}")
        
        return True
    
    def cleanup(self) -> None:
        """Clean up resources."""
        if self.spark:
            self.logger.info("Stopping Spark session...")
            self.spark.stop()
            self.spark = None