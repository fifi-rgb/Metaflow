"""
Batch runner for full table loads and batch processing.

Supports:
- Full table extraction
- Incremental batch loads
- Partition-based processing
- Delta Lake optimization
"""

import time
from typing import Dict, Any, Optional, List
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from delta.tables import DeltaTable

from metaflow.runners.base_runner import BaseRunner, RunnerConfig, PipelineMetrics, PipelineStatus


class BatchRunner(BaseRunner):
    """
    Runner for batch data pipeline execution.
    
    Features:
    - Full and incremental batch loads
    - Automatic schema evolution
    - Delta Lake optimization
    - Partition management
    - Data quality validation
    """
    
    def __init__(self, config: RunnerConfig):
        """
        Initialize batch runner.
        
        Args:
            config: Runner configuration
        """
        super().__init__(config)
        self._optimization_counter = 0
    
    def run(self, pipeline_config: Dict[str, Any]) -> PipelineMetrics:
        """
        Execute batch pipeline.
        
        Args:
            pipeline_config: Pipeline configuration containing:
                - pipeline_id: Unique identifier
                - source: Source configuration
                - target: Target Delta table path
                - query: Optional SQL query
                - partitions: Optional partition columns
                - mode: Write mode (overwrite/append/merge)
                
        Returns:
            Pipeline execution metrics
        """
        self.validate_config(pipeline_config)
        
        pipeline_id = pipeline_config['pipeline_id']
        self.logger.info(f"Starting batch pipeline: {pipeline_id}")
        
        # Initialize
        self.spark = self._initialize_spark()
        self.metrics = self._initialize_metrics(pipeline_id)
        
        try:
            # Extract data
            source_df = self._extract_data(pipeline_config)
            self.metrics.records_read = source_df.count()
            self.logger.info(f"Extracted {self.metrics.records_read} records")
            
            # Transform data
            transformed_df = self._transform_data(source_df, pipeline_config)
            
            # Validate data quality
            if self.config.enable_validation:
                validation_errors = self._validate_data(transformed_df, pipeline_config)
                self.metrics.validation_errors = validation_errors
                
                if validation_errors > 0 and self.config.fail_on_validation_error:
                    raise ValueError(f"Data validation failed with {validation_errors} errors")
            
            # Load to Delta Lake
            records_written = self._load_data(transformed_df, pipeline_config)
            self.metrics.records_written = records_written
            
            # Optimize Delta table
            self._optimize_delta_table(pipeline_config)
            
            # Save checkpoint
            self._save_checkpoint(pipeline_id, {
                'last_run': datetime.now().isoformat(),
                'records_written': records_written,
                'status': 'success'
            })
            
            self._finalize_metrics(success=True)
            self.logger.info(f"Pipeline completed successfully: {pipeline_id}")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}", exc_info=True)
            self._finalize_metrics(success=False, error=e)
            raise
        
        finally:
            self._trigger_callbacks()
            self.cleanup()
        
        return self.metrics
    
    def _extract_data(self, config: Dict[str, Any]) -> DataFrame:
        """
        Extract data from source.
        
        Args:
            config: Pipeline configuration
            
        Returns:
            Source DataFrame
        """
        source_config = config['source']
        source_type = source_config.get('type', 'jdbc')
        
        self.logger.info(f"Extracting from {source_type} source...")
        
        if source_type == 'jdbc':
            return self._extract_jdbc(source_config, config.get('query'))
        elif source_type == 'file':
            return self._extract_file(source_config)
        elif source_type == 'delta':
            return self._extract_delta(source_config)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def _extract_jdbc(self, source_config: Dict[str, Any], query: Optional[str] = None) -> DataFrame:
        """Extract data from JDBC source."""
        jdbc_url = source_config['url']
        table = source_config.get('table')
        
        options = {
            'url': jdbc_url,
            'driver': source_config.get('driver', 'org.postgresql.Driver'),
            'user': source_config.get('user', ''),
            'password': source_config.get('password', ''),
        }
        
        if query:
            options['query'] = query
        elif table:
            options['dbtable'] = table
        else:
            raise ValueError("Must specify either 'table' or 'query'")
        
        # Optimize parallel reads
        if 'partition_column' in source_config:
            options.update({
                'partitionColumn': source_config['partition_column'],
                'lowerBound': source_config.get('lower_bound', '0'),
                'upperBound': source_config.get('upper_bound', '1000000'),
                'numPartitions': source_config.get('num_partitions', self.config.max_concurrent_tasks),
            })
        
        return self.spark.read.format('jdbc').options(**options).load()
    
    def _extract_file(self, source_config: Dict[str, Any]) -> DataFrame:
        """Extract data from file source."""
        file_path = source_config['path']
        file_format = source_config.get('format', 'parquet')
        
        reader = self.spark.read.format(file_format)
        
        # Apply file-specific options
        if file_format == 'csv':
            reader = reader.option('header', source_config.get('header', True))
            reader = reader.option('inferSchema', source_config.get('inferSchema', True))
        
        return reader.load(file_path)
    
    def _extract_delta(self, source_config: Dict[str, Any]) -> DataFrame:
        """Extract data from Delta table."""
        table_path = source_config['path']
        
        df = self.spark.read.format('delta').load(table_path)
        
        # Apply version/timestamp filters if specified
        if 'version' in source_config:
            df = self.spark.read.format('delta').option('versionAsOf', source_config['version']).load(table_path)
        elif 'timestamp' in source_config:
            df = self.spark.read.format('delta').option('timestampAsOf', source_config['timestamp']).load(table_path)
        
        return df
    
    def _transform_data(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        """
        Apply transformations to data.
        
        Args:
            df: Source DataFrame
            config: Pipeline configuration
            
        Returns:
            Transformed DataFrame
        """
        # Add metadata columns
        df = df.withColumn('_metaflow_timestamp', current_timestamp())
        df = df.withColumn('_metaflow_pipeline_id', lit(config['pipeline_id']))
        
        # Apply custom transformations if specified
        if 'transformations' in config:
            for transform in config['transformations']:
                transform_type = transform['type']
                
                if transform_type == 'filter':
                    df = df.filter(transform['condition'])
                elif transform_type == 'select':
                    df = df.select(*transform['columns'])
                elif transform_type == 'rename':
                    for old_name, new_name in transform['mapping'].items():
                        df = df.withColumnRenamed(old_name, new_name)
                elif transform_type == 'cast':
                    for column, data_type in transform['mapping'].items():
                        df = df.withColumn(column, col(column).cast(data_type))
        
        return df
    
    def _validate_data(self, df: DataFrame, config: Dict[str, Any]) -> int:
        """
        Validate data quality.
        
        Args:
            df: DataFrame to validate
            config: Pipeline configuration
            
        Returns:
            Number of validation errors
        """
        errors = 0
        
        # Check for null values in required columns
        if 'required_columns' in config:
            for column in config['required_columns']:
                null_count = df.filter(col(column).isNull()).count()
                if null_count > 0:
                    self.logger.warning(f"Found {null_count} null values in required column: {column}")
                    errors += null_count
        
        # Check for duplicates
        if config.get('check_duplicates') and 'primary_key' in config:
            pk_columns = config['primary_key']
            total_count = df.count()
            distinct_count = df.select(pk_columns).distinct().count()
            duplicates = total_count - distinct_count
            
            if duplicates > 0:
                self.logger.warning(f"Found {duplicates} duplicate records")
                self.metrics.duplicate_records = duplicates
                errors += duplicates
        
        return errors
    
    def _load_data(self, df: DataFrame, config: Dict[str, Any]) -> int:
        """
        Load data to Delta Lake.
        
        Args:
            df: DataFrame to load
            config: Pipeline configuration
            
        Returns:
            Number of records written
        """
        target_path = config['target']
        mode = config.get('mode', 'append')
        partition_by = config.get('partitions', [])
        
        self.logger.info(f"Loading data to {target_path} (mode: {mode})...")
        
        writer = df.write.format('delta').mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        # Merge mode (upsert)
        if mode == 'merge' and 'merge_keys' in config:
            self._merge_data(df, config)
        else:
            writer.save(target_path)
        
        return df.count()
    
    def _merge_data(self, df: DataFrame, config: Dict[str, Any]) -> None:
        """
        Merge data into Delta table (upsert).
        
        Args:
            df: DataFrame to merge
            config: Pipeline configuration
        """
        target_path = config['target']
        merge_keys = config['merge_keys']
        
        # Create Delta table if it doesn't exist
        if not DeltaTable.isDeltaTable(self.spark, target_path):
            df.write.format('delta').save(target_path)
            return
        
        delta_table = DeltaTable.forPath(self.spark, target_path)
        
        # Build merge condition
        merge_condition = ' AND '.join([f"target.{key} = source.{key}" for key in merge_keys])
        
        # Execute merge
        delta_table.alias('target').merge(
            df.alias('source'),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        self.logger.info(f"Merged data into {target_path}")
    
    def _optimize_delta_table(self, config: Dict[str, Any]) -> None:
        """
        Optimize Delta table.
        
        Args:
            config: Pipeline configuration
        """
        self._optimization_counter += 1
        
        if self._optimization_counter % self.config.delta_optimize_interval != 0:
            return
        
        target_path = config['target']
        
        if not DeltaTable.isDeltaTable(self.spark, target_path):
            return
        
        self.logger.info(f"Optimizing Delta table: {target_path}")
        
        delta_table = DeltaTable.forPath(self.spark, target_path)
        
        # Optimize (compaction)
        delta_table.optimize().executeCompaction()
        
        # Z-order if specified
        if 'zorder_columns' in config:
            delta_table.optimize().executeZOrderBy(*config['zorder_columns'])
        
        # Vacuum old files
        retention_hours = self.config.delta_vacuum_retention_hours
        delta_table.vacuum(retention_hours)
        
        self.logger.info(f"Optimization completed for {target_path}")