"""
Utility functions for common operations.

Provides helper functions for:
- String manipulation
- Date/time handling
- File operations
- Data validation
"""

import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
import re
import json


def to_snake_case(text: str) -> str:
    """
    Convert text to snake_case.
    
    Args:
        text: Input text
        
    Returns:
        Text in snake_case
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', text)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def to_camel_case(text: str) -> str:
    """
    Convert text to camelCase.
    
    Args:
        text: Input text
        
    Returns:
        Text in camelCase
    """
    components = text.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def parse_date(
    date_str: str,
    formats: Optional[List[str]] = None
) -> Optional[datetime]:
    """
    Parse date string using multiple formats.
    
    Args:
        date_str: Date string
        formats: List of date formats to try
        
    Returns:
        Parsed datetime or None
    """
    if formats is None:
        formats = [
            '%Y-%m-%d',
            '%Y/%m/%d',
            '%d-%m-%Y',
            '%d/%m/%Y',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f'
        ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def ensure_dir(path: str) -> str:
    """
    Ensure directory exists, create if needed.
    
    Args:
        path: Directory path
        
    Returns:
        Absolute path to directory
    """
    os.makedirs(path, exist_ok=True)
    return os.path.abspath(path)


def read_json_file(file_path: str) -> Dict[str, Any]:
    """
    Read JSON file.
    
    Args:
        file_path: Path to JSON file
        
    Returns:
        Parsed JSON data
    """
    with open(file_path, 'r') as f:
        return json.load(f)


def write_json_file(
    data: Union[Dict[str, Any], List[Any]],
    file_path: str,
    pretty: bool = True
) -> None:
    """
    Write data to JSON file.
    
    Args:
        data: Data to write
        file_path: Output file path
        pretty: Whether to format JSON
    """
    with open(file_path, 'w') as f:
        if pretty:
            json.dump(data, f, indent=2)
        else:
            json.dump(data, f)


def validate_dict_keys(
    data: Dict[str, Any],
    required_keys: List[str],
    optional_keys: Optional[List[str]] = None
) -> List[str]:
    """
    Validate dictionary keys.
    
    Args:
        data: Dictionary to validate
        required_keys: Required keys
        optional_keys: Optional keys
        
    Returns:
        List of missing required keys
    """
    optional_keys = optional_keys or []
    allowed_keys = set(required_keys + optional_keys)
    actual_keys = set(data.keys())
    
    # Check for unknown keys
    unknown_keys = actual_keys - allowed_keys
    if unknown_keys:
        raise ValueError(f"Unknown keys: {unknown_keys}")
    
    # Check for missing required keys
    missing_keys = set(required_keys) - actual_keys
    return list(missing_keys)


def chunk_list(items: List[Any], size: int) -> List[List[Any]]:
    """
    Split list into chunks.
    
    Args:
        items: Input list
        size: Chunk size
        
    Returns:
        List of chunks
    """
    return [items[i:i + size] for i in range(0, len(items), size)]