"""Map database types to Spark types."""

from typing import Optional, Dict


class TypeMapper:
    """Map SQL types to PySpark types."""
    
    # Type mapping dictionaries
    TYPE_MAPPINGS: Dict[str, Dict[str, str]] = {
        "postgresql": {
            "INTEGER": "IntegerType()",
            "BIGINT": "LongType()",
            "SMALLINT": "ShortType()",
            "DECIMAL": "DecimalType({precision}, {scale})",
            "NUMERIC": "DecimalType({precision}, {scale})",
            "REAL": "FloatType()",
            "DOUBLE PRECISION": "DoubleType()",
            "VARCHAR": "StringType()",
            "CHAR": "StringType()",
            "TEXT": "StringType()",
            "BOOLEAN": "BooleanType()",
            "DATE": "DateType()",
            "TIMESTAMP": "TimestampType()",
            "TIMESTAMP WITHOUT TIME ZONE": "TimestampType()",
            "TIMESTAMP WITH TIME ZONE": "TimestampType()",
            "BYTEA": "BinaryType()",
            "UUID": "StringType()",
            "JSON": "StringType()",
            "JSONB": "StringType()",
        },
        "mysql": {
            "INT": "IntegerType()",
            "INTEGER": "IntegerType()",
            "BIGINT": "LongType()",
            "SMALLINT": "ShortType()",
            "TINYINT": "ByteType()",
            "DECIMAL": "DecimalType({precision}, {scale})",
            "NUMERIC": "DecimalType({precision}, {scale})",
            "FLOAT": "FloatType()",
            "DOUBLE": "DoubleType()",
            "VARCHAR": "StringType()",
            "CHAR": "StringType()",
            "TEXT": "StringType()",
            "BOOLEAN": "BooleanType()",
            "DATE": "DateType()",
            "DATETIME": "TimestampType()",
            "TIMESTAMP": "TimestampType()",
            "BLOB": "BinaryType()",
            "JSON": "StringType()",
        },
        "oracle": {
            "NUMBER": "DecimalType({precision}, {scale})",
            "VARCHAR2": "StringType()",
            "CHAR": "StringType()",
            "CLOB": "StringType()",
            "DATE": "TimestampType()",
            "TIMESTAMP": "TimestampType()",
            "TIMESTAMP(6)": "TimestampType()",
            "BLOB": "BinaryType()",
            "RAW": "BinaryType()",
        },
    }
    
    def __init__(self, db_type: str):
        """
        Initialize type mapper.
        
        Args:
            db_type: Database type (postgresql, mysql, oracle)
        """
        self.db_type = db_type.lower()
        self.mappings = self.TYPE_MAPPINGS.get(self.db_type, {})
    
    def map_type(
        self,
        sql_type: str,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
        length: Optional[int] = None,
    ) -> str:
        """
        Map SQL type to Spark type.
        
        Args:
            sql_type: SQL type name
            precision: Numeric precision
            scale: Numeric scale
            length: String length
            
        Returns:
            PySpark type string (e.g., "IntegerType()", "StringType()")
        """
        # Normalize type name
        sql_type_upper = sql_type.upper()
        
        # Handle special cases
        if self.db_type == "oracle" and sql_type_upper == "NUMBER":
            if scale == 0 or scale is None:
                # Integer type
                if precision is None or precision > 18:
                    return "LongType()"
                elif precision <= 10:
                    return "IntegerType()"
                else:
                    return "LongType()"
            else:
                # Decimal type
                precision = precision or 38
                scale = scale or 0
                return f"DecimalType({precision}, {scale})"
        
        # Get mapped type
        spark_type = self.mappings.get(sql_type_upper)
        
        if spark_type is None:
            # Default to StringType for unknown types
            return "StringType()"
        
        # Format with precision/scale if needed
        if "{precision}" in spark_type:
            precision = precision or 38
            scale = scale or 0
            spark_type = spark_type.format(precision=precision, scale=scale)
        
        return spark_type
    
    def to_sql_type(
        self,
        sql_type: str,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
    ) -> str:
        """
        Convert to Spark SQL type string.
        
        Args:
            sql_type: Source SQL type
            precision: Numeric precision
            scale: Numeric scale
            
        Returns:
            Spark SQL type string (e.g., "INT", "DECIMAL(10,2)", "STRING")
        """
        sql_type_upper = sql_type.upper()
        
        # Map to Spark SQL types
        if "INT" in sql_type_upper:
            if "BIG" in sql_type_upper:
                return "BIGINT"
            elif "SMALL" in sql_type_upper:
                return "SMALLINT"
            else:
                return "INT"
        elif "DECIMAL" in sql_type_upper or "NUMERIC" in sql_type_upper:
            p = precision or 38
            s = scale or 0
            return f"DECIMAL({p},{s})"
        elif "NUMBER" in sql_type_upper:
            if scale == 0 or scale is None:
                return "BIGINT"
            else:
                p = precision or 38
                s = scale or 0
                return f"DECIMAL({p},{s})"
        elif "FLOAT" in sql_type_upper or "REAL" in sql_type_upper:
            return "FLOAT"
        elif "DOUBLE" in sql_type_upper:
            return "DOUBLE"
        elif "BOOL" in sql_type_upper:
            return "BOOLEAN"
        elif "DATE" in sql_type_upper and "TIME" not in sql_type_upper:
            return "DATE"
        elif "TIME" in sql_type_upper:
            return "TIMESTAMP"
        elif "CHAR" in sql_type_upper or "TEXT" in sql_type_upper or "CLOB" in sql_type_upper:
            return "STRING"
        elif "BLOB" in sql_type_upper or "BINARY" in sql_type_upper or "RAW" in sql_type_upper:
            return "BINARY"
        else:
            return "STRING"  # Default