"""
Incremental runner for CDC and incremental updates.

Supports:
- Change Data Capture (CDC)
- Incremental timestamp-based loads
- Merge/upsert operations
- Soft deletes
"""

from typing import Dict, Any, Optional
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max as spark_max, lit
from delta.tables import DeltaTable

from metaflow.runners.base_runner import BaseRunner, RunnerConfig, PipelineMetrics


class IncrementalRunner(BaseRunner):
    """
    Runner for incremental data pipeline execution.
    
    Features:
    - CDC processing (insert/update/delete)
    - Timestamp-based incremental loads
    - Watermark tracking
    - Soft delete support
    """
    
    def run(self, pipeline_config: Dict[str, Any]) -> PipelineMetrics:
        """
        Execute incremental pipeline.
        
        Args:
            pipeline_config: Pipeline configuration containing:
                - pipeline_id: Unique identifier
                - source: Source configuration
                - target: Target Delta table path
                - incremental_column: Column for tracking (e.g., updated_at)
                - merge_keys: Primary key columns for merge
                - cdc_mode: CDC processing mode (optional)
                
        Returns:
            Pipeline execution metrics
        """
        self.validate_config(pipeline_config)
        
        pipeline_id = pipeline_config['pipeline_id']
        self.logger.info(f"Starting incremental pipeline: {pipeline_id}")
        
        # Initialize
        self.spark = self._initialize_spark()
        self.metrics = self._initialize_metrics(pipeline_id)
        
        try:
            # Get last watermark
            last_watermark = self._get_last_watermark(pipeline_config)
            self.logger.info(f"Last watermark: {last_watermark}")
            
            # Extract incremental data
            incremental_df = self._extract_incremental(pipeline_config, last_watermark)
            records_read = incremental_df.count()
            self.metrics.records_read = records_read
            
            if records_read == 0:
                self.logger.info("No new records to process")
                self._finalize_metrics(success=True)
                return self.metrics
            
            self.logger.info(f"Extracted {records_read} incremental records")
            
            # Process CDC if enabled
            if 'cdc_mode' in pipeline_config:
                processed_df = self._process_cdc(incremental_df, pipeline_config)
            else:
                processed_df = incremental_df
            
            # Merge into Delta table
            records_written = self._merge_incremental(processed_df, pipeline_config)
            self.metrics.records_written = records_written
            
            # Update watermark
            new_watermark = self._calculate_new_watermark(processed_df, pipeline_config)
            self._save_watermark(pipeline_config, new_watermark)
            
            self._finalize_metrics(success=True)
            self.logger.info(f"Incremental pipeline completed: {pipeline_id}")
            
        except Exception as e:
            self.logger.error(f"Incremental pipeline failed: {e}", exc_info=True)
            self._finalize_metrics(success=False, error=e)
            raise
        
        finally:
            self._trigger_callbacks()
            self.cleanup()
        
        return self.metrics
    
    def _get_last_watermark(self, config: Dict[str, Any]) -> Optional[str]:
        """
        Get last processed watermark.
        
        Args:
            config: Pipeline configuration
            
        Returns:
            Last watermark value or None
        """
        checkpoint = self._load_checkpoint(config['pipeline_id'])
        
        if checkpoint and 'last_watermark' in checkpoint:
            return checkpoint['last_watermark']
        
        # Try to get from target table
        target_path = config['target']
        
        if DeltaTable.isDeltaTable(self.spark, target_path):
            incremental_column = config['incremental_column']
            
            max_value = self.spark.read.format('delta').load(target_path) \
                .select(spark_max(col(incremental_column)).alias('max_value')) \
                .collect()[0]['max_value']
            
            return str(max_value) if max_value else None
        
        return None
    
    def _extract_incremental(self, config: Dict[str, Any], last_watermark: Optional[str]) -> DataFrame:
        """
        Extract incremental data from source.
        
        Args:
            config: Pipeline configuration
            last_watermark: Last processed watermark
            
        Returns:
            Incremental DataFrame
        """
        source_config = config['source']
        incremental_column = config['incremental_column']
        
        # Build query
        table = source_config.get('table')
        query = source_config.get('query')
        
        if not query:
            if last_watermark:
                query = f"SELECT * FROM {table} WHERE {incremental_column} > '{last_watermark}'"
            else:
                query = f"SELECT * FROM {table}"
        
        # Extract using JDBC
        jdbc_options = {
            'url': source_config['url'],
            'query': query,
            'driver': source_config.get('driver', 'org.postgresql.Driver'),
            'user': source_config.get('user', ''),
            'password': source_config.get('password', ''),
        }
        
        return self.spark.read.format('jdbc').options(**jdbc_options).load()
    
    def _process_cdc(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        """
        Process CDC operations.
        
        Args:
            df: CDC DataFrame
            config: Pipeline configuration
            
        Returns:
            Processed DataFrame
        """
        cdc_column = config.get('cdc_column', 'operation')
        
        # Add soft delete marker for DELETE operations
        if config.get('soft_delete', False):
            df = df.withColumn(
                '_is_deleted',
                col(cdc_column) == 'D'
            )
        else:
            # Filter out hard deletes
            df = df.filter(col(cdc_column) != 'D')
        
        return df
    
    def _merge_incremental(self, df: DataFrame, config: Dict[str, Any]) -> int:
        """
        Merge incremental data into Delta table.
        
        Args:
            df: Incremental DataFrame
            config: Pipeline configuration
            
        Returns:
            Number of records affected
        """
        target_path = config['target']
        merge_keys = config['merge_keys']
        
        # Create table if doesn't exist
        if not DeltaTable.isDeltaTable(self.spark, target_path):
            df.write.format('delta').save(target_path)
            return df.count()
        
        delta_table = DeltaTable.forPath(self.spark, target_path)
        
        # Build merge condition
        merge_condition = ' AND '.join([f"target.{key} = source.{key}" for key in merge_keys])
        
        # Execute merge
        merge_builder = delta_table.alias('target').merge(
            df.alias('source'),
            merge_condition
        )
        
        # Handle soft deletes
        if '_is_deleted' in df.columns:
            merge_builder = merge_builder \
                .whenMatchedUpdate(
                    condition="source._is_deleted = true",
                    set={'_is_deleted': lit(True)}
                ) \
                .whenMatchedUpdateAll(condition="source._is_deleted = false")
        else:
            merge_builder = merge_builder.whenMatchedUpdateAll()
        
        merge_builder.whenNotMatchedInsertAll().execute()
        
        return df.count()
    
    def _calculate_new_watermark(self, df: DataFrame, config: Dict[str, Any]) -> str:
        """
        Calculate new watermark value.
        
        Args:
            df: Processed DataFrame
            config: Pipeline configuration
            
        Returns:
            New watermark value
        """
        incremental_column = config['incremental_column']
        
        max_value = df.select(spark_max(col(incremental_column)).alias('max_value')) \
            .collect()[0]['max_value']
        
        return str(max_value)
    
    def _save_watermark(self, config: Dict[str, Any], watermark: str) -> None:
        """
        Save new watermark value.
        
        Args:
            config: Pipeline configuration
            watermark: New watermark value
        """
        checkpoint_state = {
            'last_watermark': watermark,
            'last_run': datetime.now().isoformat(),
        }
        
        self._save_checkpoint(config['pipeline_id'], checkpoint_state)
        self.logger.info(f"Watermark updated: {watermark}")