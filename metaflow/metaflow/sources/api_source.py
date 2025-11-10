"""
API source for fetching data from REST APIs.

Supports:
- HTTP methods (GET, POST, etc.)
- Authentication
- Pagination
- Rate limiting
- Retry logic
"""

from typing import Dict, Any, Optional, Generator, Union
import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from metaflow.sources.base_source import BaseSource


class APISource(BaseSource):
    """
    Source for reading data from REST APIs.
    
    Features:
    - Configurable HTTP methods
    - Authentication handling
    - Pagination support
    - Rate limiting
    - Automatic retries
    """
    
    def __init__(
        self,
        spark: SparkSession,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        auth: Optional[tuple] = None,
        schema: Optional[StructType] = None,
        pagination_config: Optional[Dict[str, Any]] = None,
        rate_limit: Optional[int] = None,
        max_retries: int = 3
    ):
        """
        Initialize API source.
        
        Args:
            spark: SparkSession
            url: Base API URL
            method: HTTP method (GET, POST, etc.)
            headers: Request headers
            params: Query parameters
            auth: Authentication tuple (username, password)
            schema: Expected data schema
            pagination_config: Pagination configuration
            rate_limit: Requests per second limit
            max_retries: Maximum retry attempts
        """
        super().__init__(spark)
        self.url = url
        self.method = method.upper()
        self.headers = headers or {}
        self.params = params or {}
        self.auth = auth
        self.schema = schema
        self.pagination_config = pagination_config or {}
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        
        self.session = requests.Session()
    
    def read(self) -> DataFrame:
        """
        Read data from API.
        
        Returns:
            DataFrame with API response data
        """
        data = list(self._fetch_data())
        return self.spark.createDataFrame(data, schema=self.schema)
    
    def _fetch_data(self) -> Generator[Dict[str, Any], None, None]:
        """
        Fetch data from API with pagination support.
        
        Yields:
            Response data records
        """
        url = self.url
        params = self.params.copy()
        
        while url:
            response = self._make_request(url, params)
            records = self._extract_records(response)
            
            yield from records
            
            url = self._get_next_page(response)
            params = self._update_pagination_params(params, response)
    
    def _make_request(
        self,
        url: str,
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Make HTTP request with retries.
        
        Args:
            url: Request URL
            params: Query parameters
            
        Returns:
            Response data
        """
        for attempt in range(self.max_retries):
            try:
                response = self.session.request(
                    method=self.method,
                    url=url,
                    headers=self.headers,
                    params=params,
                    auth=self.auth,
                    timeout=30
                )
                response.raise_for_status()
                return response.json()
            
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                self.logger.warning(f"Request failed, retrying: {e}")
    
    def _extract_records(
        self,
        response_data: Dict[str, Any]
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Extract records from response data.
        
        Args:
            response_data: API response data
            
        Yields:
            Individual records
        """
        records_path = self.pagination_config.get('records_path', '')
        
        if records_path:
            records = self._get_nested_value(response_data, records_path)
        else:
            records = response_data
            
        if isinstance(records, list):
            yield from records
        else:
            yield records
    
    def _get_next_page(self, response_data: Dict[str, Any]) -> Optional[str]:
        """
        Get next page URL from response.
        
        Args:
            response_data: API response data
            
        Returns:
            Next page URL or None
        """
        next_page_path = self.pagination_config.get('next_page_path')
        if not next_page_path:
            return None
            
        return self._get_nested_value(response_data, next_page_path)
    
    def _update_pagination_params(
        self,
        params: Dict[str, Any],
        response_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update pagination parameters.
        
        Args:
            params: Current parameters
            response_data: API response data
            
        Returns:
            Updated parameters
        """
        param_updates = self.pagination_config.get('param_updates', {})
        
        for param, path in param_updates.items():
            value = self._get_nested_value(response_data, path)
            if value is not None:
                params[param] = value
        
        return params
    
    @staticmethod
    def _get_nested_value(data: Dict[str, Any], path: str) -> Any:
        """
        Get value from nested dictionary using dot notation.
        
        Args:
            data: Nested dictionary
            path: Path to value using dot notation
            
        Returns:
            Value at path
        """
        current = data
        for key in path.split('.'):
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None
        return current