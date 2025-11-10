"""
Delta Lake source for reading Delta tables.

Supports:
- Time travel (version and timestamp-based)
- Schema evolution
- Predicate pushdown
- Partition pruning
"""

from typing import Optional, Dict, Any, Union
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame

from metaflow.sources.base_source import BaseSource


class DeltaSource(BaseSource):
    """
    Source for reading Delta Lake tables.
    
    Features:
    - Version-based time travel
    - Timestamp-based time travel
    - Schema evolution
    - Optimized reads
    """
    
    def __init__(
        self,
        spark: SparkSession,
        table_path: str,
        version: Optional[int] = None,
        timestamp: Optional[Union[datetime, str]] = None,
        options: Optional[Dict[str, str]] = None
    ):
        """
        Initialize Delta source.
        
        Args:
            spark: SparkSession
            table_path: Path to Delta table
            version: Table version to read (optional)
            timestamp: Timestamp to read (optional)
            options: Additional Delta read options
        """
        super().__init__(spark)
        self.table_path = table_path
        self.version = version
        self.timestamp = timestamp
        self.options = options or {}
    
    def read(self) -> DataFrame:
        """
        Read Delta table.
        
        Returns:
            DataFrame with table data
        """
        reader = self.spark.read.format("delta").options(**self.options)
        
        if self.version is not None:
            reader = reader.option("versionAsOf", str(self.version))
        elif self.timestamp is not None:
            if isinstance(self.timestamp, datetime):
                timestamp_str = self.timestamp.isoformat()
            else:
                timestamp_str = self.timestamp
            reader = reader.option("timestampAsOf", timestamp_str)
        
        return reader.load(self.table_path)
    
    def get_table_details(self) -> Dict[str, Any]:
        """
        Get Delta table details.
        
        Returns:
            Dictionary with table information
        """
        from delta.tables import DeltaTable
        
        delta_table = DeltaTable.forPath(self.spark, self.table_path)
        
        # Get table history
        history = delta_table.history()
        
        # Get current metadata
        metadata = delta_table.detail().select("*").collect()[0].asDict()
        
        return {
            "metadata": metadata,
            "latest_version": history.select("version").first()[0],
            "latest_timestamp": history.select("timestamp").first()[0],
            "num_files": metadata.get("numFiles", 0),
            "size_bytes": metadata.get("sizeInBytes", 0),
            "partitioned_by": metadata.get("partitionColumns", [])
        }