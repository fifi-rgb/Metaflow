"""
Schema validator for DataFrame schema validation and evolution.

Supports:
- Schema comparison
- Schema evolution
- Column type validation
- Required column checks
"""

from enum import Enum
from typing import Dict, Any, Optional, List, Set

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, DataType
from pyspark.sql.functions import lit, col

from metaflow.transformers.base_transformer import BaseTransformer, TransformerConfig


class SchemaEvolutionMode(Enum):
    """Schema evolution modes."""
    STRICT = "strict"  # No schema changes allowed
    ADD_COLUMNS = "add_columns"  # Allow new columns
    EVOLVE = "evolve"  # Allow all compatible changes
    

class SchemaValidator(BaseTransformer):
    """
    Validator for DataFrame schemas.
    
    Features:
    - Schema comparison and validation
    - Schema evolution handling
    - Required column checks
    - Data type validation
    """
    
    def __init__(
        self,
        spark,
        config: TransformerConfig,
        expected_schema: Optional[StructType] = None,
        required_columns: Optional[List[str]] = None,
        evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.STRICT
    ):
        """
        Initialize schema validator.
        
        Args:
            spark: Active SparkSession
            config: Transformer configuration
            expected_schema: Expected DataFrame schema
            required_columns: List of required column names
            evolution_mode: Schema evolution handling mode
        """
        super().__init__(spark, config)
        self.expected_schema = expected_schema
        self.required_columns = required_columns or []
        self.evolution_mode = evolution_mode
    
    def _do_transform(self, df: DataFrame) -> DataFrame:
        """
        Validate and potentially evolve schema.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with validated/evolved schema
        """
        # Check required columns
        self._validate_required_columns(df)
        
        # Validate schema if expected schema is provided
        if self.expected_schema:
            df = self._validate_schema(df)
        
        return df
    
    def _validate_required_columns(self, df: DataFrame) -> None:
        """
        Validate that all required columns are present.
        
        Args:
            df: Input DataFrame
        """
        actual_columns = set(df.columns)
        required_set = set(self.required_columns)
        
        missing_columns = required_set - actual_columns
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        self.logger.info(f"All {len(self.required_columns)} required columns present")
    
    def _validate_schema(self, df: DataFrame) -> DataFrame:
        """
        Validate DataFrame schema against expected schema.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with validated/evolved schema
        """
        actual_schema = df.schema
        
        if self.evolution_mode == SchemaEvolutionMode.STRICT:
            return self._validate_strict(df, actual_schema)
        elif self.evolution_mode == SchemaEvolutionMode.ADD_COLUMNS:
            return self._validate_add_columns(df, actual_schema)
        elif self.evolution_mode == SchemaEvolutionMode.EVOLVE:
            return self._validate_evolve(df, actual_schema)
        else:
            raise ValueError(f"Unsupported evolution mode: {self.evolution_mode}")
    
    def _validate_strict(self, df: DataFrame, actual_schema: StructType) -> DataFrame:
        """
        Strict schema validation - schemas must match exactly.
        
        Args:
            df: Input DataFrame
            actual_schema: Actual schema
            
        Returns:
            Original DataFrame if valid
        """
        if actual_schema != self.expected_schema:
            differences = self._get_schema_differences(actual_schema, self.expected_schema)
            raise ValueError(f"Schema mismatch: {differences}")
        
        self.logger.info("Schema validation passed (strict mode)")
        return df
    
    def _validate_add_columns(self, df: DataFrame, actual_schema: StructType) -> DataFrame:
        """
        Allow new columns to be added.
        
        Args:
            df: Input DataFrame
            actual_schema: Actual schema
            
        Returns:
            DataFrame with aligned schema
        """
        expected_fields = {field.name: field for field in self.expected_schema.fields}
        actual_fields = {field.name: field for field in actual_schema.fields}
        
        # Check that all expected columns exist and have correct types
        for col_name, expected_field in expected_fields.items():
            if col_name not in actual_fields:
                raise ValueError(f"Missing expected column: {col_name}")
            
            actual_field = actual_fields[col_name]
            if actual_field.dataType != expected_field.dataType:
                raise ValueError(
                    f"Column '{col_name}' type mismatch: "
                    f"expected {expected_field.dataType}, got {actual_field.dataType}"
                )
        
        # New columns are allowed
        new_columns = set(actual_fields.keys()) - set(expected_fields.keys())
        if new_columns:
            self.logger.info(f"New columns detected: {new_columns}")
        
        return df
    
    def _validate_evolve(self, df: DataFrame, actual_schema: StructType) -> DataFrame:
        """
        Allow schema evolution with type casting.
        
        Args:
            df: Input DataFrame
            actual_schema: Actual schema
            
        Returns:
            DataFrame with evolved schema
        """
        expected_fields = {field.name: field for field in self.expected_schema.fields}
        actual_fields = {field.name: field for field in actual_schema.fields}
        
        result_df = df
        
        # Add missing columns with null values
        for col_name, expected_field in expected_fields.items():
            if col_name not in actual_fields:
                self.logger.info(f"Adding missing column '{col_name}' with null values")
                result_df = result_df.withColumn(col_name, lit(None).cast(expected_field.dataType))
        
        # Cast columns to expected types if different
        for col_name in actual_fields.keys():
            if col_name in expected_fields:
                expected_type = expected_fields[col_name].dataType
                actual_type = actual_fields[col_name].dataType
                
                if actual_type != expected_type:
                    self.logger.info(
                        f"Casting column '{col_name}' from {actual_type} to {expected_type}"
                    )
                    result_df = result_df.withColumn(col_name, col(col_name).cast(expected_type))
        
        return result_df
    
    def _get_schema_differences(
        self,
        schema1: StructType,
        schema2: StructType
    ) -> Dict[str, Any]:
        """
        Get differences between two schemas.
        
        Args:
            schema1: First schema
            schema2: Second schema
            
        Returns:
            Dictionary describing differences
        """
        fields1 = {field.name: field for field in schema1.fields}
        fields2 = {field.name: field for field in schema2.fields}
        
        missing_in_1 = set(fields2.keys()) - set(fields1.keys())
        missing_in_2 = set(fields1.keys()) - set(fields2.keys())
        
        type_mismatches = {}
        for col_name in set(fields1.keys()) & set(fields2.keys()):
            if fields1[col_name].dataType != fields2[col_name].dataType:
                type_mismatches[col_name] = {
                    'actual': str(fields1[col_name].dataType),
                    'expected': str(fields2[col_name].dataType)
                }
        
        return {
            'missing_columns': list(missing_in_1),
            'extra_columns': list(missing_in_2),
            'type_mismatches': type_mismatches
        }
    
    def compare_schemas(
        self,
        schema1: StructType,
        schema2: StructType
    ) -> bool:
        """
        Compare two schemas for equality.
        
        Args:
            schema1: First schema
            schema2: Second schema
            
        Returns:
            True if schemas are equal
        """
        return schema1 == schema2