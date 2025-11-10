"""
Aggregation transformer for data aggregation operations.

Supports:
- Group by aggregations
- Window functions
- Pivot operations
- Rollup and cube
"""

from typing import Dict, Any, Optional, List
from enum import Enum

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum, avg, count, min, max, stddev, variance,
    first, last, collect_list, collect_set, approx_count_distinct
)
from pyspark.sql.window import Window

from metaflow.transformers.base_transformer import BaseTransformer, TransformerConfig


class AggregationType(Enum):
    """Aggregation function types."""
    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    MIN = "min"
    MAX = "max"
    STDDEV = "stddev"
    VARIANCE = "variance"
    FIRST = "first"
    LAST = "last"
    COLLECT_LIST = "collect_list"
    COLLECT_SET = "collect_set"
    COUNT_DISTINCT = "count_distinct"


class AggregationTransformer(BaseTransformer):
    """
    Transformer for data aggregation operations.
    
    Features:
    - Multiple aggregation functions
    - Group by aggregations
    - Window functions
    - Pivot and unpivot
    - Rollup and cube
    """
    
    def __init__(
        self,
        spark,
        config: TransformerConfig,
        group_by_columns: List[str],
        aggregations: Dict[str, List[str]]
    ):
        """
        Initialize aggregation transformer.
        
        Args:
            spark: Active SparkSession
            config: Transformer configuration
            group_by_columns: Columns to group by
            aggregations: Dictionary mapping aggregation types to column lists
                Example: {
                    'sum': ['sales', 'quantity'],
                    'avg': ['price'],
                    'count': ['customer_id']
                }
        """
        super().__init__(spark, config)
        self.group_by_columns = group_by_columns
        self.aggregations = aggregations
    
    def _do_transform(self, df: DataFrame) -> DataFrame:
        """
        Apply aggregation operations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Aggregated DataFrame
        """
        # Start with group by
        grouped = df.groupBy(*self.group_by_columns)
        
        # Build aggregation expressions
        agg_exprs = []
        
        for agg_type, columns in self.aggregations.items():
            agg_func = self._get_aggregation_function(agg_type)
            
            for column in columns:
                if column not in df.columns:
                    self.logger.warning(f"Column '{column}' not found in DataFrame")
                    continue
                
                expr = agg_func(col(column)).alias(f"{column}_{agg_type}")
                agg_exprs.append(expr)
        
        if not agg_exprs:
            raise ValueError("No valid aggregation expressions found")
        
        # Apply aggregations
        result_df = grouped.agg(*agg_exprs)
        
        self.logger.info(
            f"Aggregated data by {len(self.group_by_columns)} columns "
            f"with {len(agg_exprs)} aggregations"
        )
        
        return result_df
    
    def _get_aggregation_function(self, agg_type: str):
        """
        Get Spark aggregation function by type.
        
        Args:
            agg_type: Aggregation type name
            
        Returns:
            Spark aggregation function
        """
        agg_map = {
            'sum': sum,
            'avg': avg,
            'count': count,
            'min': min,
            'max': max,
            'stddev': stddev,
            'variance': variance,
            'first': first,
            'last': last,
            'collect_list': collect_list,
            'collect_set': collect_set,
            'count_distinct': approx_count_distinct,
        }
        
        if agg_type not in agg_map:
            raise ValueError(f"Unsupported aggregation type: {agg_type}")
        
        return agg_map[agg_type]
    
    def transform_with_window(
        self,
        df: DataFrame,
        partition_by: List[str],
        order_by: Optional[List[str]] = None,
        window_aggregations: Optional[Dict[str, List[str]]] = None
    ) -> DataFrame:
        """
        Apply window aggregations.
        
        Args:
            df: Input DataFrame
            partition_by: Columns to partition by
            order_by: Columns to order by (optional)
            window_aggregations: Window aggregation specifications
            
        Returns:
            DataFrame with window aggregations
        """
        # Define window
        window_spec = Window.partitionBy(*partition_by)
        
        if order_by:
            window_spec = window_spec.orderBy(*order_by)
        
        result_df = df
        
        # Apply window aggregations
        if window_aggregations:
            for agg_type, columns in window_aggregations.items():
                agg_func = self._get_aggregation_function(agg_type)
                
                for column in columns:
                    window_col_name = f"{column}_{agg_type}_window"
                    result_df = result_df.withColumn(
                        window_col_name,
                        agg_func(col(column)).over(window_spec)
                    )
        
        return result_df
    
    def transform_with_pivot(
        self,
        df: DataFrame,
        pivot_column: str,
        value_column: str,
        agg_func: str = 'sum'
    ) -> DataFrame:
        """
        Pivot data from long to wide format.
        
        Args:
            df: Input DataFrame
            pivot_column: Column to pivot on
            value_column: Column containing values
            agg_func: Aggregation function to apply
            
        Returns:
            Pivoted DataFrame
        """
        agg_function = self._get_aggregation_function(agg_func)
        
        result_df = df.groupBy(*self.group_by_columns) \
            .pivot(pivot_column) \
            .agg(agg_function(col(value_column)))
        
        self.logger.info(f"Pivoted data on column '{pivot_column}'")
        
        return result_df
    
    def transform_with_rollup(self, df: DataFrame) -> DataFrame:
        """
        Apply rollup aggregation (hierarchical totals).
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with rollup aggregations
        """
        # Build aggregation expressions
        agg_exprs = []
        
        for agg_type, columns in self.aggregations.items():
            agg_func = self._get_aggregation_function(agg_type)
            
            for column in columns:
                expr = agg_func(col(column)).alias(f"{column}_{agg_type}")
                agg_exprs.append(expr)
        
        result_df = df.rollup(*self.group_by_columns).agg(*agg_exprs)
        
        self.logger.info("Applied rollup aggregation")
        
        return result_df
    
    def transform_with_cube(self, df: DataFrame) -> DataFrame:
        """
        Apply cube aggregation (all combinations).
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with cube aggregations
        """
        # Build aggregation expressions
        agg_exprs = []
        
        for agg_type, columns in self.aggregations.items():
            agg_func = self._get_aggregation_function(agg_type)
            
            for column in columns:
                expr = agg_func(col(column)).alias(f"{column}_{agg_type}")
                agg_exprs.append(expr)
        
        result_df = df.cube(*self.group_by_columns).agg(*agg_exprs)
        
        self.logger.info("Applied cube aggregation")
        
        return result_df
    
    def transform_with_percentiles(
        self,
        df: DataFrame,
        column: str,
        percentiles: List[float] = [0.25, 0.5, 0.75]
    ) -> DataFrame:
        """
        Calculate percentiles for a column.
        
        Args:
            df: Input DataFrame
            column: Column to calculate percentiles for
            percentiles: List of percentiles (0.0 to 1.0)
            
        Returns:
            DataFrame with percentile values
        """
        from pyspark.sql.functions import expr
        
        # Calculate percentiles using approx_percentile
        percentile_cols = []
        for p in percentiles:
            col_name = f"{column}_p{int(p*100)}"
            percentile_expr = expr(f"approx_percentile({column}, {p})").alias(col_name)
            percentile_cols.append(percentile_expr)
        
        result_df = df.groupBy(*self.group_by_columns).agg(*percentile_cols)
        
        return result_df