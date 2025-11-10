"""
Type casting transformer for data type conversion.

Supports:
- Schema-based type casting
- Custom type conversion rules
- Null handling
- Error tracking for failed conversions
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
from pyspark.sql.types import DataType, StringType, IntegerType, FloatType, BooleanType, TimestampType

from metaflow.transformers.base_transformer import BaseTransformer, TransformerConfig


class TypeCastTransformer(BaseTransformer):
    """
    Transformer for type casting operations.
    
    Features:
    - Schema-based type casting
    - Custom type conversion rules
    - Null value handling
    - Failed conversion tracking
    """
    
    def __init__(
        self,
        spark,
        config: TransformerConfig,
        type_mappings: Dict[str, DataType],
        track_failed_conversions: bool = True,
        error_handling: str = "keep"  # keep, null, fail
    ):
        """
        Initialize type casting transformer.
        
        Args:
            spark: Active SparkSession
            config: Transformer configuration
            type_mappings: Dictionary mapping column names to target types
            track_failed_conversions: Whether to track failed conversions
            error_handling: How to handle failed conversions
        """
        super().__init__(spark, config)
        self.type_mappings = type_mappings
        self.track_failed_conversions = track_failed_conversions
        self.error_handling = error_handling
        self.conversion_metrics: Dict[str, Dict[str, int]] = {}
    
    def _do_transform(self, df: DataFrame) -> DataFrame:
        """
        Apply type casting to DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with cast types
        """
        result_df = df
        
        for column, target_type in self.type_mappings.items():
            if column not in df.columns:
                self.logger.warning(f"Column '{column}' not found in DataFrame")
                continue
            
            self.logger.info(f"Casting column '{column}' to {target_type}")
            
            # Track original column for failed conversion detection
            if self.track_failed_conversions:
                temp_col = f"__{column}_original"
                result_df = result_df.withColumn(temp_col, col(column))
            
            # Apply type casting
            cast_col = col(column).cast(target_type)
            
            if self.error_handling == "keep":
                result_df = result_df.withColumn(column, cast_col)
            elif self.error_handling == "null":
                # Handle failed conversions by setting to null
                result_df = result_df.withColumn(
                    column,
                    when(cast_col.isNotNull(), cast_col).otherwise(None)
                )
            elif self.error_handling == "fail":
                # Check for failed conversions
                failed = result_df.filter(
                    col(column).isNotNull() & cast_col.isNull()
                )
                if failed.count() > 0:
                    raise ValueError(
                        f"Type casting failed for {failed.count()} values in column '{column}'"
                    )
                result_df = result_df.withColumn(column, cast_col)
            else:
                raise ValueError(f"Invalid error_handling mode: {self.error_handling}")
            
            # Track failed conversions
            if self.track_failed_conversions:
                failed_count = result_df.filter(
                    col(temp_col).isNotNull() & col(column).isNull()
                ).count()
                
                success_count = result_df.filter(col(column).isNotNull()).count()
                
                self.conversion_metrics[column] = {
                    'success': success_count,
                    'failed': failed_count
                }
                
                # Clean up temporary column
                result_df = result_df.drop(temp_col)
        
        # Log conversion metrics
        if self.track_failed_conversions:
            self.logger.info(f"Conversion metrics: {self.conversion_metrics}")
        
        return result_df
    
    @staticmethod
    def get_supported_types() -> Dict[str, DataType]:
        """
        Get dictionary of supported data types.
        
        Returns:
            Dictionary mapping type names to Spark DataTypes
        """
        return {
            'string': StringType(),
            'integer': IntegerType(),
            'float': FloatType(),
            'boolean': BooleanType(),
            'timestamp': TimestampType()
        }
    
    def get_conversion_metrics(self) -> Dict[str, Dict[str, int]]:
        """
        Get metrics about type conversions.
        
        Returns:
            Dictionary with conversion success/failure counts per column
        """
        return self.conversion_metrics
