"""Extract metadata from source databases."""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.engine import Engine
import logging

logger = logging.getLogger(__name__)


@dataclass
class ColumnMetadata:
    """Metadata for a single column."""
    name: str
    data_type: str
    nullable: bool
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    default: Optional[str] = None


@dataclass
class TableMetadata:
    """Metadata for a single table."""
    name: str
    schema: str
    columns: List[ColumnMetadata] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    indexes: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class DatabaseMetadata:
    """Complete database metadata."""
    database_type: str
    database_name: str
    tables: List[TableMetadata] = field(default_factory=list)


class MetadataExtractor:
    """Extract metadata from various database sources."""
    
    def __init__(self, connection_string: str):
        """
        Initialize metadata extractor.
        
        Args:
            connection_string: SQLAlchemy connection string
                Examples:
                - postgresql://user:pass@localhost:5432/mydb
                - mysql://user:pass@localhost:3306/mydb
                - oracle://user:pass@localhost:1521/orcl
        """
        self.connection_string = connection_string
        self.engine: Engine = create_engine(connection_string)
        self.db_type = self.engine.dialect.name
        
        logger.info(f"Connected to {self.db_type} database")
    
    def extract(self, table_names: Optional[List[str]] = None) -> DatabaseMetadata:
        """
        Extract metadata from database.
        
        Args:
            table_names: List of specific tables to extract, or None for all tables
            
        Returns:
            DatabaseMetadata object with complete schema information
        """
        inspector = inspect(self.engine)
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        
        db_metadata = DatabaseMetadata(
            database_type=self.db_type,
            database_name=self.engine.url.database or "unknown"
        )
        
        # Get all tables or filtered list
        tables_to_process = table_names if table_names else inspector.get_table_names()
        
        for table_name in tables_to_process:
            try:
                table_meta = self._extract_table(inspector, table_name)
                db_metadata.tables.append(table_meta)
                logger.info(f"Extracted metadata for table: {table_name}")
            except Exception as e:
                logger.error(f"Error extracting {table_name}: {e}")
        
        return db_metadata
    
    def _extract_table(self, inspector, table_name: str) -> TableMetadata:
        """Extract metadata for a single table."""
        
        # Get columns
        columns = []
        for col in inspector.get_columns(table_name):
            col_meta = ColumnMetadata(
                name=col["name"],
                data_type=str(col["type"]),
                nullable=col["nullable"],
                default=col.get("default"),
            )
            
            # Extract length/precision/scale if available
            if hasattr(col["type"], "length"):
                col_meta.length = col["type"].length
            if hasattr(col["type"], "precision"):
                col_meta.precision = col["type"].precision
            if hasattr(col["type"], "scale"):
                col_meta.scale = col["type"].scale
            
            columns.append(col_meta)
        
        # Get primary keys
        pk_constraint = inspector.get_pk_constraint(table_name)
        primary_keys = pk_constraint.get("constrained_columns", [])
        
        # Get indexes
        indexes = inspector.get_indexes(table_name)
        
        return TableMetadata(
            name=table_name,
            schema=inspector.default_schema_name or "public",
            columns=columns,
            primary_keys=primary_keys,
            indexes=indexes,
        )
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False