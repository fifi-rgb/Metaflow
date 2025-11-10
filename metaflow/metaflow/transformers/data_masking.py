"""
Data masking transformer for PII protection and anonymization.

Supports:
- Email masking
- Phone number masking
- Credit card masking
- Custom masking patterns
- Hashing and encryption
"""

from enum import Enum
from typing import Dict, Any, Optional, List, Callable
import hashlib

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, regexp_replace, sha2, md5, concat, lit
from pyspark.sql.types import StringType

from metaflow.transformers.base_transformer import BaseTransformer, TransformerConfig


class MaskingStrategy(Enum):
    """Data masking strategies."""
    FULL_MASK = "full_mask"  # Replace with fixed characters
    PARTIAL_MASK = "partial_mask"  # Mask part of the value
    HASH = "hash"  # Hash the value
    ENCRYPT = "encrypt"  # Encrypt the value
    TOKENIZE = "tokenize"  # Replace with token
    REDACT = "redact"  # Remove completely
    FORMAT_PRESERVE = "format_preserve"  # Maintain format while masking


class DataMaskingTransformer(BaseTransformer):
    """
    Transformer for data masking and anonymization.
    
    Features:
    - Multiple masking strategies
    - PII detection and masking
    - Format-preserving encryption
    - Reversible tokenization
    """
    
    def __init__(
        self,
        spark,
        config: TransformerConfig,
        masking_rules: Dict[str, Dict[str, Any]]
    ):
        """
        Initialize data masking transformer.
        
        Args:
            spark: Active SparkSession
            config: Transformer configuration
            masking_rules: Dictionary mapping column names to masking configurations
                Example: {
                    'email': {'strategy': 'PARTIAL_MASK', 'preserve_domain': True},
                    'ssn': {'strategy': 'HASH'},
                    'credit_card': {'strategy': 'PARTIAL_MASK', 'visible_digits': 4}
                }
        """
        super().__init__(spark, config)
        self.masking_rules = masking_rules
        self._token_map: Dict[str, str] = {}
    
    def _do_transform(self, df: DataFrame) -> DataFrame:
        """
        Apply data masking rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with masked columns
        """
        masked_df = df
        
        for column, rule_config in self.masking_rules.items():
            if column not in df.columns:
                self.logger.warning(f"Column '{column}' not found in DataFrame")
                continue
            
            strategy_name = rule_config.get('strategy', 'FULL_MASK')
            strategy = MaskingStrategy[strategy_name]
            
            self.logger.info(f"Masking column '{column}' with strategy '{strategy.value}'")
            masked_df = self._apply_masking(masked_df, column, strategy, rule_config)
        
        return masked_df
    
    def _apply_masking(
        self,
        df: DataFrame,
        column: str,
        strategy: MaskingStrategy,
        config: Dict[str, Any]
    ) -> DataFrame:
        """
        Apply masking strategy to a column.
        
        Args:
            df: Input DataFrame
            column: Column name to mask
            strategy: Masking strategy
            config: Strategy configuration
            
        Returns:
            DataFrame with masked column
        """
        if strategy == MaskingStrategy.FULL_MASK:
            return self._mask_full(df, column, config)
        elif strategy == MaskingStrategy.PARTIAL_MASK:
            return self._mask_partial(df, column, config)
        elif strategy == MaskingStrategy.HASH:
            return self._mask_hash(df, column, config)
        elif strategy == MaskingStrategy.REDACT:
            return self._mask_redact(df, column)
        elif strategy == MaskingStrategy.TOKENIZE:
            return self._mask_tokenize(df, column, config)
        elif strategy == MaskingStrategy.FORMAT_PRESERVE:
            return self._mask_format_preserve(df, column, config)
        else:
            raise ValueError(f"Unsupported masking strategy: {strategy}")
    
    def _mask_full(self, df: DataFrame, column: str, config: Dict[str, Any]) -> DataFrame:
        """
        Replace entire value with mask character.
        
        Args:
            df: Input DataFrame
            column: Column to mask
            config: Configuration with 'mask_char' (default: '*')
            
        Returns:
            DataFrame with fully masked column
        """
        mask_char = config.get('mask_char', '*')
        mask_length = config.get('mask_length', 10)
        
        return df.withColumn(column, lit(mask_char * mask_length))
    
    def _mask_partial(self, df: DataFrame, column: str, config: Dict[str, Any]) -> DataFrame:
        """
        Mask part of the value while preserving some characters.
        
        Args:
            df: Input DataFrame
            column: Column to mask
            config: Configuration with masking parameters
            
        Returns:
            DataFrame with partially masked column
        """
        mask_char = config.get('mask_char', '*')
        
        # For email addresses
        if config.get('preserve_domain', False):
            return self._mask_email(df, column, mask_char)
        
        # For credit cards
        if 'visible_digits' in config:
            return self._mask_credit_card(df, column, config['visible_digits'], mask_char)
        
        # For phone numbers
        if config.get('preserve_area_code', False):
            return self._mask_phone(df, column, mask_char)
        
        # Generic partial masking
        visible_start = config.get('visible_start', 0)
        visible_end = config.get('visible_end', 0)
        
        mask_udf = udf(
            lambda x: self._partial_mask_value(x, visible_start, visible_end, mask_char),
            StringType()
        )
        
        return df.withColumn(column, mask_udf(col(column)))
    
    def _mask_email(self, df: DataFrame, column: str, mask_char: str) -> DataFrame:
        """Mask email addresses preserving domain."""
        def mask_email_value(email):
            if not email or '@' not in email:
                return email
            
            local, domain = email.split('@', 1)
            if len(local) <= 2:
                masked_local = mask_char * len(local)
            else:
                masked_local = local[0] + (mask_char * (len(local) - 2)) + local[-1]
            
            return f"{masked_local}@{domain}"
        
        mask_email_udf = udf(mask_email_value, StringType())
        return df.withColumn(column, mask_email_udf(col(column)))
    
    def _mask_credit_card(
        self,
        df: DataFrame,
        column: str,
        visible_digits: int,
        mask_char: str
    ) -> DataFrame:
        """Mask credit card numbers showing only last N digits."""
        def mask_cc_value(cc):
            if not cc:
                return cc
            
            # Remove spaces and dashes
            cc_clean = cc.replace(' ', '').replace('-', '')
            
            if len(cc_clean) < visible_digits:
                return mask_char * len(cc_clean)
            
            masked = (mask_char * (len(cc_clean) - visible_digits)) + cc_clean[-visible_digits:]
            
            # Format with spaces every 4 digits
            return ' '.join([masked[i:i+4] for i in range(0, len(masked), 4)])
        
        mask_cc_udf = udf(mask_cc_value, StringType())
        return df.withColumn(column, mask_cc_udf(col(column)))
    
    def _mask_phone(self, df: DataFrame, column: str, mask_char: str) -> DataFrame:
        """Mask phone numbers preserving area code."""
        def mask_phone_value(phone):
            if not phone:
                return phone
            
            # Remove non-numeric characters
            digits = ''.join(filter(str.isdigit, phone))
            
            if len(digits) == 10:
                # Format: (XXX) XXX-XXXX -> (XXX) ***-****
                return f"({digits[:3]}) {mask_char*3}-{mask_char*4}"
            elif len(digits) == 11:
                # Format: +1 (XXX) XXX-XXXX -> +1 (XXX) ***-****
                return f"+{digits[0]} ({digits[1:4]}) {mask_char*3}-{mask_char*4}"
            else:
                return mask_char * len(digits)
        
        mask_phone_udf = udf(mask_phone_value, StringType())
        return df.withColumn(column, mask_phone_udf(col(column)))
    
    def _partial_mask_value(
        self,
        value: str,
        visible_start: int,
        visible_end: int,
        mask_char: str
    ) -> str:
        """Generic partial masking logic."""
        if not value:
            return value
        
        length = len(value)
        
        if visible_start + visible_end >= length:
            return value
        
        start = value[:visible_start] if visible_start > 0 else ''
        end = value[-visible_end:] if visible_end > 0 else ''
        middle = mask_char * (length - visible_start - visible_end)
        
        return start + middle + end
    
    def _mask_hash(self, df: DataFrame, column: str, config: Dict[str, Any]) -> DataFrame:
        """
        Hash column values.
        
        Args:
            df: Input DataFrame
            column: Column to hash
            config: Configuration with 'algorithm' (sha256, md5)
            
        Returns:
            DataFrame with hashed column
        """
        algorithm = config.get('algorithm', 'sha256')
        salt = config.get('salt', '')
        
        if algorithm == 'sha256':
            if salt:
                return df.withColumn(column, sha2(concat(col(column), lit(salt)), 256))
            else:
                return df.withColumn(column, sha2(col(column), 256))
        elif algorithm == 'md5':
            if salt:
                return df.withColumn(column, md5(concat(col(column), lit(salt))))
            else:
                return df.withColumn(column, md5(col(column)))
        else:
            raise ValueError(f"Unsupported hash algorithm: {algorithm}")
    
    def _mask_redact(self, df: DataFrame, column: str) -> DataFrame:
        """
        Completely redact column (set to null or constant).
        
        Args:
            df: Input DataFrame
            column: Column to redact
            
        Returns:
            DataFrame with redacted column
        """
        return df.withColumn(column, lit("[REDACTED]"))
    
    def _mask_tokenize(self, df: DataFrame, column: str, config: Dict[str, Any]) -> DataFrame:
        """
        Replace values with tokens (reversible).
        
        Args:
            df: Input DataFrame
            column: Column to tokenize
            config: Configuration
            
        Returns:
            DataFrame with tokenized column
        """
        def tokenize_value(value):
            if not value:
                return value
            
            if value not in self._token_map:
                # Generate token
                token_prefix = config.get('token_prefix', 'TOK')
                token_id = len(self._token_map) + 1
                self._token_map[value] = f"{token_prefix}_{token_id:08d}"
            
            return self._token_map[value]
        
        tokenize_udf = udf(tokenize_value, StringType())
        return df.withColumn(column, tokenize_udf(col(column)))
    
    def _mask_format_preserve(
        self,
        df: DataFrame,
        column: str,
        config: Dict[str, Any]
    ) -> DataFrame:
        """
        Format-preserving masking (maintains character types).
        
        Args:
            df: Input DataFrame
            column: Column to mask
            config: Configuration
            
        Returns:
            DataFrame with format-preserved masking
        """
        def format_preserve_mask(value):
            if not value:
                return value
            
            import random
            import string
            
            result = []
            for char in value:
                if char.isdigit():
                    result.append(str(random.randint(0, 9)))
                elif char.isalpha():
                    if char.isupper():
                        result.append(random.choice(string.ascii_uppercase))
                    else:
                        result.append(random.choice(string.ascii_lowercase))
                else:
                    result.append(char)
            
            return ''.join(result)
        
        mask_udf = udf(format_preserve_mask, StringType())
        return df.withColumn(column, mask_udf(col(column)))
    
    def get_token_map(self) -> Dict[str, str]:
        """
        Get the token mapping for reversing tokenization.
        
        Returns:
            Dictionary mapping original values to tokens
        """
        return self._token_map.copy()
    
    def detokenize(self, df: DataFrame, column: str) -> DataFrame:
        """
        Reverse tokenization using stored token map.
        
        Args:
            df: Input DataFrame with tokenized column
            column: Column to detokenize
            
        Returns:
            DataFrame with detokenized column
        """
        # Create reverse mapping
        reverse_map = {token: original for original, token in self._token_map.items()}
        
        def detokenize_value(token):
            return reverse_map.get(token, token)
        
        detokenize_udf = udf(detokenize_value, StringType())
        return df.withColumn(column, detokenize_udf(col(column)))