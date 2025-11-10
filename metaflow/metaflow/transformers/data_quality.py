"""
Data quality transformer for validation and cleansing.

Supports:
- Null checks
- Data type validation
- Range validation
- Pattern matching
- Custom quality rules
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Any, Optional, List, Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, isnull, length

from metaflow.transformers.base_transformer import BaseTransformer, TransformerConfig


class RuleType(Enum):
    """Quality rule types."""
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    RANGE = "range"
    PATTERN = "pattern"
    CUSTOM = "custom"
    ALLOWED_VALUES = "allowed_values"
    LENGTH = "length"


@dataclass
class QualityRule:
    """Data quality rule definition."""
    
    name: str
    column: str
    rule_type: RuleType
    parameters: Dict[str, Any]
    severity: str = "error"  # error, warning
    
    def __post_init__(self):
        if self.severity not in ['error', 'warning']:
            raise ValueError(f"Invalid severity: {self.severity}")


class DataQualityTransformer(BaseTransformer):
    """
    Transformer for data quality validation and cleansing.
    
    Features:
    - Multiple validation rules
    - Automatic data cleansing
    - Quality metrics reporting
    - Invalid record quarantine
    """
    
    def __init__(
        self,
        spark,
        config: TransformerConfig,
        rules: List[QualityRule],
        quarantine_invalid: bool = False,
        quarantine_path: Optional[str] = None
    ):
        """
        Initialize data quality transformer.
        
        Args:
            spark: Active SparkSession
            config: Transformer configuration
            rules: List of quality rules to apply
            quarantine_invalid: Whether to quarantine invalid records
            quarantine_path: Path for quarantine data
        """
        super().__init__(spark, config)
        self.rules = rules
        self.quarantine_invalid = quarantine_invalid
        self.quarantine_path = quarantine_path
        self.quality_metrics: Dict[str, Any] = {}
    
    def _do_transform(self, df: DataFrame) -> DataFrame:
        """
        Apply data quality rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        validated_df = df
        invalid_records = None
        
        for rule in self.rules:
            self.logger.info(f"Applying rule: {rule.name}")
            
            # Apply validation rule
            validated_df, rule_invalid = self._apply_rule(validated_df, rule)
            
            # Collect invalid records
            if rule_invalid is not None and rule_invalid.count() > 0:
                if invalid_records is None:
                    invalid_records = rule_invalid
                else:
                    invalid_records = invalid_records.union(rule_invalid)
        
        # Quarantine invalid records if configured
        if self.quarantine_invalid and invalid_records is not None and self.quarantine_path:
            self._quarantine_records(invalid_records)
        
        # Collect quality metrics
        self._collect_quality_metrics(df, validated_df)
        
        return validated_df
    
    def _apply_rule(self, df: DataFrame, rule: QualityRule) -> tuple:
        """
        Apply single quality rule.
        
        Args:
            df: Input DataFrame
            rule: Quality rule to apply
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        if rule.rule_type == RuleType.NOT_NULL:
            return self._apply_not_null_rule(df, rule)
        elif rule.rule_type == RuleType.UNIQUE:
            return self._apply_unique_rule(df, rule)
        elif rule.rule_type == RuleType.RANGE:
            return self._apply_range_rule(df, rule)
        elif rule.rule_type == RuleType.PATTERN:
            return self._apply_pattern_rule(df, rule)
        elif rule.rule_type == RuleType.ALLOWED_VALUES:
            return self._apply_allowed_values_rule(df, rule)
        elif rule.rule_type == RuleType.LENGTH:
            return self._apply_length_rule(df, rule)
        elif rule.rule_type == RuleType.CUSTOM:
            return self._apply_custom_rule(df, rule)
        else:
            raise ValueError(f"Unsupported rule type: {rule.rule_type}")
    
    def _apply_not_null_rule(self, df: DataFrame, rule: QualityRule) -> tuple:
        """Apply NOT NULL validation."""
        valid_df = df.filter(col(rule.column).isNotNull())
        invalid_df = df.filter(col(rule.column).isNull())
        
        invalid_count = invalid_df.count()
        if invalid_count > 0:
            self.logger.warning(f"Rule '{rule.name}': Found {invalid_count} null values")
        
        if rule.severity == "error":
            return valid_df, invalid_df
        else:
            return df, None
    
    def _apply_unique_rule(self, df: DataFrame, rule: QualityRule) -> tuple:
        """Apply UNIQUE validation."""
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        window = Window.partitionBy(rule.column).orderBy(rule.column)
        df_with_rn = df.withColumn("_rn", row_number().over(window))
        
        valid_df = df_with_rn.filter(col("_rn") == 1).drop("_rn")
        invalid_df = df_with_rn.filter(col("_rn") > 1).drop("_rn")
        
        invalid_count = invalid_df.count()
        if invalid_count > 0:
            self.logger.warning(f"Rule '{rule.name}': Found {invalid_count} duplicate values")
        
        return valid_df, invalid_df if rule.severity == "error" else None
    
    def _apply_range_rule(self, df: DataFrame, rule: QualityRule) -> tuple:
        """Apply RANGE validation."""
        min_val = rule.parameters.get('min')
        max_val = rule.parameters.get('max')
        
        condition = lit(True)
        if min_val is not None:
            condition = condition & (col(rule.column) >= min_val)
        if max_val is not None:
            condition = condition & (col(rule.column) <= max_val)
        
        valid_df = df.filter(condition)
        invalid_df = df.filter(~condition)
        
        invalid_count = invalid_df.count()
        if invalid_count > 0:
            self.logger.warning(
                f"Rule '{rule.name}': Found {invalid_count} out-of-range values"
            )
        
        return valid_df, invalid_df if rule.severity == "error" else None
    
    def _apply_pattern_rule(self, df: DataFrame, rule: QualityRule) -> tuple:
        """Apply PATTERN validation."""
        pattern = rule.parameters['pattern']
        
        valid_df = df.filter(col(rule.column).rlike(pattern))
        invalid_df = df.filter(~col(rule.column).rlike(pattern))
        
        invalid_count = invalid_df.count()
        if invalid_count > 0:
            self.logger.warning(
                f"Rule '{rule.name}': Found {invalid_count} pattern violations"
            )
        
        return valid_df, invalid_df if rule.severity == "error" else None
    
    def _apply_allowed_values_rule(self, df: DataFrame, rule: QualityRule) -> tuple:
        """Apply ALLOWED VALUES validation."""
        allowed_values = rule.parameters['values']
        
        valid_df = df.filter(col(rule.column).isin(allowed_values))
        invalid_df = df.filter(~col(rule.column).isin(allowed_values))
        
        invalid_count = invalid_df.count()
        if invalid_count > 0:
            self.logger.warning(
                f"Rule '{rule.name}': Found {invalid_count} disallowed values"
            )
        
        return valid_df, invalid_df if rule.severity == "error" else None
    
    def _apply_length_rule(self, df: DataFrame, rule: QualityRule) -> tuple:
        """Apply LENGTH validation."""
        min_length = rule.parameters.get('min_length')
        max_length = rule.parameters.get('max_length')
        
        condition = lit(True)
        if min_length is not None:
            condition = condition & (length(col(rule.column)) >= min_length)
        if max_length is not None:
            condition = condition & (length(col(rule.column)) <= max_length)
        
        valid_df = df.filter(condition)
        invalid_df = df.filter(~condition)
        
        invalid_count = invalid_df.count()
        if invalid_count > 0:
            self.logger.warning(
                f"Rule '{rule.name}': Found {invalid_count} length violations"
            )
        
        return valid_df, invalid_df if rule.severity == "error" else None
    
    def _apply_custom_rule(self, df: DataFrame, rule: QualityRule) -> tuple:
        """Apply CUSTOM validation."""
        validator: Callable = rule.parameters['validator']
        
        valid_df = df.filter(validator(col(rule.column)))
        invalid_df = df.filter(~validator(col(rule.column)))
        
        return valid_df, invalid_df if rule.severity == "error" else None
    
    def _quarantine_records(self, invalid_df: DataFrame) -> None:
        """
        Quarantine invalid records.
        
        Args:
            invalid_df: DataFrame with invalid records
        """
        from pyspark.sql.functions import current_timestamp
        
        quarantine_df = invalid_df.withColumn('_quarantine_timestamp', current_timestamp())
        
        try:
            quarantine_df.write.mode('append').parquet(self.quarantine_path)
            count = invalid_df.count()
            self.logger.info(f"Quarantined {count} invalid records to {self.quarantine_path}")
        except Exception as e:
            self.logger.error(f"Failed to quarantine records: {e}")
    
    def _collect_quality_metrics(self, original_df: DataFrame, cleaned_df: DataFrame) -> None:
        """
        Collect quality metrics.
        
        Args:
            original_df: Original DataFrame
            cleaned_df: Cleaned DataFrame
        """
        total_records = original_df.count()
        valid_records = cleaned_df.count()
        invalid_records = total_records - valid_records
        
        self.quality_metrics = {
            'total_records': total_records,
            'valid_records': valid_records,
            'invalid_records': invalid_records,
            'quality_score': (valid_records / total_records * 100) if total_records > 0 else 0,
        }
        
        self.logger.info(f"Quality metrics: {self.quality_metrics}")
    
    def get_quality_metrics(self) -> Dict[str, Any]:
        """
        Get quality metrics.
        
        Returns:
            Quality metrics dictionary
        """
        return self.quality_metrics