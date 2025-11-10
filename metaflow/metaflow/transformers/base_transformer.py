"""
Base transformer class for all MetaFlow transformers.

Provides common functionality for data transformations.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional

from pyspark.sql import DataFrame, SparkSession


@dataclass
class TransformerConfig:
    """Configuration for transformers."""
    
    # Execution settings
    enable_caching: bool = False
    cache_level: str = "MEMORY_AND_DISK"
    
    # Error handling
    error_handling_mode: str = "fail"  # fail, skip, log
    max_errors: int = 100
    
    # Performance
    repartition_count: Optional[int] = None
    coalesce_count: Optional[int] = None
    
    # Logging
    enable_metrics: bool = True
    log_level: str = "INFO"
    sample_data_for_logging: bool = False
    
    # Custom configurations
    custom_configs: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TransformationMetrics:
    """Metrics collected during transformation."""
    
    transformer_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    
    # Record counts
    input_records: int = 0
    output_records: int = 0
    filtered_records: int = 0
    error_records: int = 0
    
    # Performance
    duration_seconds: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            'transformer_name': self.transformer_name,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'input_records': self.input_records,
            'output_records': self.output_records,
            'filtered_records': self.filtered_records,
            'error_records': self.error_records,
            'duration_seconds': self.duration_seconds,
        }


class BaseTransformer(ABC):
    """
    Abstract base class for all transformers.
    
    Provides common functionality for:
    - Data transformation
    - Metrics collection
    - Error handling
    - Caching management
    """
    
    def __init__(self, spark: SparkSession, config: TransformerConfig):
        """
        Initialize transformer.
        
        Args:
            spark: Active SparkSession
            config: Transformer configuration
        """
        self.spark = spark
        self.config = config
        self.logger = self._setup_logging()
        self.metrics: Optional[TransformationMetrics] = None
    
    def _setup_logging(self) -> logging.Logger:
        """Configure logging for the transformer."""
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
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform DataFrame with metrics collection.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.metrics = TransformationMetrics(
            transformer_name=self.__class__.__name__,
            start_time=datetime.now()
        )
        
        self.metrics.input_records = df.count()
        self.logger.info(f"Transforming {self.metrics.input_records} records")
        
        try:
            # Apply caching if enabled
            if self.config.enable_caching:
                df = df.persist(getattr(df.storageLevel, self.config.cache_level))
            
            # Apply transformation
            result_df = self._do_transform(df)
            
            # Apply repartitioning if configured
            if self.config.repartition_count:
                result_df = result_df.repartition(self.config.repartition_count)
            elif self.config.coalesce_count:
                result_df = result_df.coalesce(self.config.coalesce_count)
            
            self.metrics.output_records = result_df.count()
            self.metrics.filtered_records = self.metrics.input_records - self.metrics.output_records
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {e}", exc_info=True)
            raise
        
        finally:
            self._finalize_metrics()
            if self.config.enable_caching:
                df.unpersist()
        
        return result_df
    
    @abstractmethod
    def _do_transform(self, df: DataFrame) -> DataFrame:
        """
        Perform the actual transformation.
        
        Subclasses must implement this method.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        pass
    
    def _finalize_metrics(self) -> None:
        """Finalize metrics after transformation."""
        if not self.metrics:
            return
        
        self.metrics.end_time = datetime.now()
        self.metrics.duration_seconds = (
            self.metrics.end_time - self.metrics.start_time
        ).total_seconds()
        
        self.logger.info(f"Transformation metrics: {self.metrics.to_dict()}")
    
    def get_metrics(self) -> Optional[TransformationMetrics]:
        """
        Get transformation metrics.
        
        Returns:
            TransformationMetrics or None
        """
        return self.metrics