"""
Deduplication transformer for removing duplicate records.

Supports:
- Full row deduplication
- Key-based deduplication
- Latest record selection
- Custom deduplication logic
"""

from enum import Enum
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, desc

from metaflow.transformers.base_transformer import BaseTransformer, TransformerConfig


class DeduplicationStrategy(Enum):
    """Deduplication strategies."""
    FIRST = "first"  # Keep first occurrence
    LAST = "last"  # Keep last occurrence
    LATEST_TIMESTAMP = "latest_timestamp"  # Keep record with latest timestamp


class Deduplicator(BaseTransformer):
    """
    Transformer for removing duplicate records.
    
    Features:
    - Multiple deduplication strategies
    - Key-based or full-row deduplication
    - Configurable sort order
    - Duplicate record tracking
    """
    
    def __init__(
        self,
        spark,
        config: TransformerConfig,
        key_columns: Optional[List[str]] = None,
        strategy: DeduplicationStrategy = DeduplicationStrategy.FIRST,
        timestamp_column: Optional[str] = None,
        order_by_columns: Optional[List[str]] = None
    ):
        """
        Initialize deduplication transformer.
        
        Args:
            spark: Active SparkSession
            config: Transformer configuration
            key_columns: Columns to identify duplicates (None = all columns)
            strategy: Deduplication strategy
            timestamp_column: Column for timestamp-based dedup
            order_by_columns: Columns for ordering duplicates
        """
        super().__init__(spark, config)
        self.key_columns = key_columns
        self.strategy = strategy
        self.timestamp_column = timestamp_column
        self.order_by_columns = order_by_columns or []
        self.duplicate_count = 0
    
    def _do_transform(self, df: DataFrame) -> DataFrame:
        """
        Remove duplicate records.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Deduplicated DataFrame
        """
        if self.key_columns is None:
            # Full row deduplication
            return self._deduplicate_full_row(df)
        else:
            # Key-based deduplication
            return self._deduplicate_by_keys(df)
    
    def _deduplicate_full_row(self, df: DataFrame) -> DataFrame:
        """
        Deduplicate based on all columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Deduplicated DataFrame
        """
        original_count = df.count()
        deduplicated_df = df.dropDuplicates()
        deduplicated_count = deduplicated_df.count()
        
        self.duplicate_count = original_count - deduplicated_count
        self.logger.info(f"Removed {self.duplicate_count} full-row duplicates")
        
        return deduplicated_df
    
    def _deduplicate_by_keys(self, df: DataFrame) -> DataFrame:
        """
        Deduplicate based on key columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Deduplicated DataFrame
        """
        if self.strategy == DeduplicationStrategy.FIRST:
            return self._keep_first(df)
        elif self.strategy == DeduplicationStrategy.LAST:
            return self._keep_last(df)
        elif self.strategy == DeduplicationStrategy.LATEST_TIMESTAMP:
            return self._keep_latest_timestamp(df)
        else:
            raise ValueError(f"Unsupported strategy: {self.strategy}")
    
    def _keep_first(self, df: DataFrame) -> DataFrame:
        """Keep first occurrence of duplicates."""
        window = Window.partitionBy(*self.key_columns).orderBy(*self.order_by_columns) \
            if self.order_by_columns else Window.partitionBy(*self.key_columns)
        
        df_with_rn = df.withColumn("_row_num", row_number().over(window))
        deduplicated_df = df_with_rn.filter(col("_row_num") == 1).drop("_row_num")
        
        self.duplicate_count = df.count() - deduplicated_df.count()
        self.logger.info(f"Removed {self.duplicate_count} duplicates (kept first)")
        
        return deduplicated_df
    
    def _keep_last(self, df: DataFrame) -> DataFrame:
        """Keep last occurrence of duplicates."""
        # Reverse the order
        order_cols = [desc(c) for c in self.order_by_columns] if self.order_by_columns else []
        window = Window.partitionBy(*self.key_columns).orderBy(*order_cols) \
            if order_cols else Window.partitionBy(*self.key_columns)
        
        df_with_rn = df.withColumn("_row_num", row_number().over(window))
        deduplicated_df = df_with_rn.filter(col("_row_num") == 1).drop("_row_num")
        
        self.duplicate_count = df.count() - deduplicated_df.count()
        self.logger.info(f"Removed {self.duplicate_count} duplicates (kept last)")
        
        return deduplicated_df
    
    def _keep_latest_timestamp(self, df: DataFrame) -> DataFrame:
        """Keep record with latest timestamp."""
        if not self.timestamp_column:
            raise ValueError("timestamp_column required for LATEST_TIMESTAMP strategy")
        
        window = Window.partitionBy(*self.key_columns).orderBy(desc(self.timestamp_column))
        
        df_with_rn = df.withColumn("_row_num", row_number().over(window))
        deduplicated_df = df_with_rn.filter(col("_row_num") == 1).drop("_row_num")
        
        self.duplicate_count = df.count() - deduplicated_df.count()
        self.logger.info(f"Removed {self.duplicate_count} duplicates (kept latest)")
        
        return deduplicated_df
    
    def get_duplicate_count(self) -> int:
        """
        Get number of duplicates removed.
        
        Returns:
            Duplicate count
        """
        return self.duplicate_count


# Alias for backward compatibility
DeduplicationTransformer = Deduplicator