"""
File source for reading various file formats.

Supports:
- CSV, JSON, Parquet, ORC
- Schema inference and enforcement
- Compression
- Partition discovery
"""

from typing import Optional, Dict, List, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from metaflow.sources.base_source import BaseSource


class FileSource(BaseSource):
    """
    Source for reading files in various formats.
    
    Features:
    - Multiple format support
    - Schema handling
    - Compression support
    - Partition handling
    """
    
    SUPPORTED_FORMATS = {
        'csv': 'com.databricks.spark.csv',
        'json': 'json',
        'parquet': 'parquet',
        'orc': 'orc',
        'text': 'text',
        'avro': 'avro'
    }
    
    def __init__(
        self,
        spark: SparkSession,
        path: str,
        format: str = 'parquet',
        schema: Optional[StructType] = None,
        options: Optional[Dict[str, str]] = None
    ):
        """
        Initialize file source.
        
        Args:
            spark: SparkSession
            path: Path to file(s)
            format: File format
            schema: Optional schema
            options: Additional read options
        """
        super().__init__(spark)
        
        if format.lower() not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {format}")
        
        self.path = path
        self.format = format.lower()
        self.schema = schema
        self.options = options or {}
    
    def read(self) -> DataFrame:
        """
        Read file(s).
        
        Returns:
            DataFrame with file contents
        """
        reader = self.spark.read.format(self.SUPPORTED_FORMATS[self.format])
        
        if self.schema:
            reader = reader.schema(self.schema)
        
        if self.options:
            reader = reader.options(**self.options)
        
        return reader.load(self.path)
    
    def get_partitions(self) -> List[Dict[str, str]]:
        """
        Get partition information.
        
        Returns:
            List of partition specifications
        """
        df = self.read()
        return [
            {col: value for col, value in zip(df.columns, partition)}
            for partition in df.select(df.partitionBy()).distinct().collect()
        ]
    
    @classmethod
    def get_supported_formats(cls) -> List[str]:
        """
        Get list of supported file formats.
        
        Returns:
            List of format names
        """
        return list(cls.SUPPORTED_FORMATS.keys())
    
    def estimate_size(self) -> Dict[str, Any]:
        """
        Estimate data size.
        
        Returns:
            Dictionary with size information
        """
        import os
        
        total_size = 0
        num_files = 0
        
        # Handle both single file and directory
        if os.path.isfile(self.path):
            total_size = os.path.getsize(self.path)
            num_files = 1
        else:
            for dirpath, _, filenames in os.walk(self.path):
                for f in filenames:
                    if not f.startswith('.'):  # Skip hidden files
                        fp = os.path.join(dirpath, f)
                        total_size += os.path.getsize(fp)
                        num_files += 1
        
        return {
            "total_size_bytes": total_size,
            "num_files": num_files,
            "avg_file_size_bytes": total_size / num_files if num_files > 0 else 0
        }