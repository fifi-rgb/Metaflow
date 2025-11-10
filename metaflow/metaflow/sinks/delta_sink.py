"""
Delta Lake sink for writing to Delta tables.

Features:
- Append, overwrite, and merge operations
- Schema evolution and validation
- Partition management
- Table optimization
- Z-ordering
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

from metaflow.sinks.base_sink import BaseSink, SinkConfig, WriteMode


class DeltaSink(BaseSink):
    """
    Sink for writing data to Delta Lake tables.
    
    Supports:
    - All Delta Lake write modes
    - Schema evolution
    - Merge/upsert operations
    - Partition pruning
    - Table optimization (compact, Z-order)
    """
    
    def __init__(self, spark, config: SinkConfig, table_path: str):
        """
        Initialize Delta sink.
        
        Args:
            spark: Active SparkSession
            config: Sink configuration
            table_path: Path to Delta table
        """
        super().__init__(spark, config)
        self.table_path = table_path
        self._optimize_counter = 0
    
    def _do_write(self, df: DataFrame, **kwargs) -> None:
        """
        Write DataFrame to Delta table.
        
        Args:
            df: DataFrame to write
            **kwargs: Additional options including:
                - merge_keys: List of columns for merge operation
                - replace_where: Condition for partition replacement
                - optimize: Whether to optimize after write
                - zorder_columns: Columns for Z-ordering
        """
        merge_keys = kwargs.get('merge_keys')
        replace_where = kwargs.get('replace_where')
        
        if self.config.write_mode == WriteMode.MERGE and merge_keys:
            self._merge_data(df, merge_keys, **kwargs)
        elif replace_where:
            self._replace_partition(df, replace_where)
        else:
            self._standard_write(df)
        
        # Optimize if requested
        if kwargs.get('optimize', False):
            self._optimize_table(kwargs.get('zorder_columns'))
    
    def _standard_write(self, df: DataFrame) -> None:
        """
        Perform standard Delta write operation.
        
        Args:
            df: DataFrame to write
        """
        writer = df.write.format('delta').mode(self.config.write_mode.value)
        
        # Apply partitioning
        if self.config.partition_columns:
            writer = writer.partitionBy(*self.config.partition_columns)
        
        # Schema evolution
        if self.config.enable_schema_evolution:
            writer = writer.option('mergeSchema', 'true')
        
        # Compression
        if self.config.enable_compression:
            writer = writer.option('compression', self.config.compression_codec)
        
        # Apply custom options
        for key, value in self.config.custom_options.items():
            writer = writer.option(key, value)
        
        writer.save(self.table_path)
        
        self.logger.info(f"Wrote data to Delta table: {self.table_path}")
    
    def _merge_data(self, df: DataFrame, merge_keys: List[str], **kwargs) -> None:
        """
        Merge data into Delta table (upsert).
        
        Args:
            df: DataFrame to merge
            merge_keys: Columns to match on
            **kwargs: Additional merge options
        """
        # Create table if it doesn't exist
        if not DeltaTable.isDeltaTable(self.spark, self.table_path):
            self.logger.info(f"Creating new Delta table: {self.table_path}")
            self._standard_write(df)
            return
        
        delta_table = DeltaTable.forPath(self.spark, self.table_path)
        
        # Build merge condition
        merge_condition = ' AND '.join([f"target.{key} = source.{key}" for key in merge_keys])
        
        # Build merge operation
        merge_builder = delta_table.alias('target').merge(
            df.alias('source'),
            merge_condition
        )
        
        # Handle delete condition
        delete_condition = kwargs.get('delete_condition')
        if delete_condition:
            merge_builder = merge_builder.whenMatchedDelete(condition=delete_condition)
        
        # Update matched records
        update_condition = kwargs.get('update_condition')
        update_set = kwargs.get('update_set', {})
        
        if update_set:
            merge_builder = merge_builder.whenMatchedUpdate(
                condition=update_condition,
                set=update_set
            )
        else:
            merge_builder = merge_builder.whenMatchedUpdateAll(condition=update_condition)
        
        # Insert new records
        insert_condition = kwargs.get('insert_condition')
        insert_values = kwargs.get('insert_values', {})
        
        if insert_values:
            merge_builder = merge_builder.whenNotMatchedInsert(
                condition=insert_condition,
                values=insert_values
            )
        else:
            merge_builder = merge_builder.whenNotMatchedInsertAll(condition=insert_condition)
        
        # Execute merge
        merge_builder.execute()
        
        self.logger.info(f"Merged data into Delta table: {self.table_path}")
    
    def _replace_partition(self, df: DataFrame, condition: str) -> None:
        """
        Replace specific partitions.
        
        Args:
            df: DataFrame to write
            condition: Partition condition (e.g., "date = '2024-01-01'")
        """
        writer = df.write.format('delta') \
            .mode('overwrite') \
            .option('replaceWhere', condition)
        
        if self.config.partition_columns:
            writer = writer.partitionBy(*self.config.partition_columns)
        
        writer.save(self.table_path)
        
        self.logger.info(f"Replaced partition where {condition}")
    
    def _optimize_table(self, zorder_columns: Optional[List[str]] = None) -> None:
        """
        Optimize Delta table.
        
        Args:
            zorder_columns: Optional columns for Z-ordering
        """
        if not DeltaTable.isDeltaTable(self.spark, self.table_path):
            return
        
        self._optimize_counter += 1
        
        delta_table = DeltaTable.forPath(self.spark, self.table_path)
        
        self.logger.info(f"Optimizing Delta table: {self.table_path}")
        
        # Compact small files
        if zorder_columns:
            delta_table.optimize().executeZOrderBy(*zorder_columns)
            self.logger.info(f"Z-ordered by columns: {zorder_columns}")
        else:
            delta_table.optimize().executeCompaction()
        
        self.logger.info("Table optimization completed")
    
    def vacuum(self, retention_hours: int = 168) -> None:
        """
        Vacuum Delta table to remove old files.
        
        Args:
            retention_hours: Retention period in hours (default: 7 days)
        """
        if not DeltaTable.isDeltaTable(self.spark, self.table_path):
            return
        
        delta_table = DeltaTable.forPath(self.spark, self.table_path)
        
        self.logger.info(f"Vacuuming Delta table with retention {retention_hours}h")
        delta_table.vacuum(retention_hours)
        self.logger.info("Vacuum completed")
    
    def get_table_history(self, limit: int = 20) -> DataFrame:
        """
        Get Delta table history.
        
        Args:
            limit: Number of historical versions to return
            
        Returns:
            DataFrame with table history
        """
        if not DeltaTable.isDeltaTable(self.spark, self.table_path):
            raise ValueError(f"Not a Delta table: {self.table_path}")
        
        delta_table = DeltaTable.forPath(self.spark, self.table_path)
        return delta_table.history(limit)
    
    def restore_version(self, version: int) -> None:
        """
        Restore table to specific version.
        
        Args:
            version: Version number to restore
        """
        if not DeltaTable.isDeltaTable(self.spark, self.table_path):
            raise ValueError(f"Not a Delta table: {self.table_path}")
        
        delta_table = DeltaTable.forPath(self.spark, self.table_path)
        
        self.logger.info(f"Restoring table to version {version}")
        delta_table.restoreToVersion(version)
        self.logger.info("Table restored successfully")