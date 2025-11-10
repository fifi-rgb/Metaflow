"""Generate Spark schemas from database metadata."""

from typing import List, Dict
from metaflow.core.metadata_extractor import TableMetadata, ColumnMetadata
from metaflow.core.type_mapper import TypeMapper


class SchemaGenerator:
    """Generate PySpark schemas from database metadata."""
    
    def __init__(self, db_type: str):
        """
        Initialize schema generator.
        
        Args:
            db_type: Database type (postgresql, mysql, oracle, etc.)
        """
        self.type_mapper = TypeMapper(db_type)
    
    def generate_struct_type(self, table: TableMetadata) -> str:
        """
        Generate PySpark StructType schema code.
        
        Args:
            table: Table metadata
            
        Returns:
            Python code string for StructType schema
        """
        fields = []
        
        for col in table.columns:
            spark_type = self.type_mapper.map_type(
                col.data_type,
                col.precision,
                col.scale,
                col.length
            )
            nullable = str(col.nullable)
            fields.append(
                f'    StructField("{col.name}", {spark_type}, {nullable})'
            )
        
        schema_code = "StructType([\n"
        schema_code += ",\n".join(fields)
        schema_code += "\n])"
        
        return schema_code
    
    def generate_ddl(self, table: TableMetadata, delta_path: str) -> str:
        """
        Generate Delta Lake DDL statement.
        
        Args:
            table: Table metadata
            delta_path: Path for Delta table
            
        Returns:
            SQL DDL statement
        """
        columns = []
        
        for col in table.columns:
            spark_type = self.type_mapper.to_sql_type(
                col.data_type,
                col.precision,
                col.scale
            )
            nullable = "" if col.nullable else "NOT NULL"
            columns.append(f"    {col.name} {spark_type} {nullable}".strip())
        
        ddl = f"CREATE TABLE IF NOT EXISTS {table.name} (\n"
        ddl += ",\n".join(columns)
        ddl += "\n) USING DELTA\n"
        ddl += f"LOCATION '{delta_path}/{table.name}'\n"
        
        # Add partitioning if table has created_date or similar
        date_columns = [
            col.name for col in table.columns 
            if any(x in col.name.lower() for x in ['created', 'updated', 'date'])
        ]
        if date_columns:
            ddl += f"PARTITIONED BY (year({date_columns[0]}))"
        
        return ddl
    
    def generate_primary_key_condition(self, table: TableMetadata) -> str:
        """
        Generate join condition for primary keys.
        
        Args:
            table: Table metadata
            
        Returns:
            SQL join condition string
        """
        if not table.primary_keys:
            return "FALSE"  # No PK defined
        
        conditions = [
            f"source.{pk} = target.{pk}" 
            for pk in table.primary_keys
        ]
        
        return " AND ".join(conditions)