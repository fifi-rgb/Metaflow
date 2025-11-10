"""
REST API sink for writing to HTTP endpoints.

Supports:
- Batch API calls
- Rate limiting
- Retry logic
- Authentication
"""

import json
import time
from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame, Row
import requests

from metaflow.sinks.base_sink import BaseSink, SinkConfig


class ApiSink(BaseSink):
    """
    Sink for writing data to REST APIs.
    
    Supports:
    - POST/PUT/PATCH methods
    - Batch requests
    - Rate limiting
    - Retry with exponential backoff
    - Authentication (Bearer, Basic, API Key)
    """
    
    def __init__(
        self,
        spark,
        config: SinkConfig,
        endpoint_url: str,
        method: str = 'POST',
        auth_config: Optional[Dict[str, str]] = None,
        rate_limit: Optional[int] = None
    ):
        """
        Initialize API sink.
        
        Args:
            spark: Active SparkSession
            config: Sink configuration
            endpoint_url: API endpoint URL
            method: HTTP method (POST, PUT, PATCH)
            auth_config: Authentication configuration
            rate_limit: Max requests per second
        """
        super().__init__(spark, config)
        self.endpoint_url = endpoint_url
        self.method = method.upper()
        self.auth_config = auth_config or {}
        self.rate_limit = rate_limit
        self._last_request_time = 0
    
    def _do_write(self, df: DataFrame, **kwargs) -> None:
        """
        Write DataFrame to API endpoint.
        
        Args:
            df: DataFrame to write
            **kwargs: Additional API options
        """
        # Collect data (careful with large datasets)
        records = df.collect()
        
        self.logger.info(f"Sending {len(records)} records to API: {self.endpoint_url}")
        
        batch_size = kwargs.get('batch_size', self.config.batch_size)
        
        # Process in batches
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            self._send_batch(batch, **kwargs)
    
    def _send_batch(self, batch: List[Row], **kwargs) -> None:
        """
        Send batch of records to API.
        
        Args:
            batch: List of Row objects
            **kwargs: Additional request options
        """
        # Apply rate limiting
        if self.rate_limit:
            self._apply_rate_limit()
        
        # Convert batch to JSON
        payload = [row.asDict() for row in batch]
        
        # Prepare headers
        headers = self._prepare_headers(kwargs)
        
        # Make request with retry
        self._make_request_with_retry(payload, headers)
    
    def _prepare_headers(self, kwargs: Dict[str, Any]) -> Dict[str, str]:
        """
        Prepare HTTP headers including authentication.
        
        Args:
            kwargs: Additional header options
            
        Returns:
            Headers dictionary
        """
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        
        # Add authentication
        if 'bearer_token' in self.auth_config:
            headers['Authorization'] = f"Bearer {self.auth_config['bearer_token']}"
        elif 'api_key' in self.auth_config:
            headers['X-API-Key'] = self.auth_config['api_key']
        
        # Add custom headers
        if 'headers' in kwargs:
            headers.update(kwargs['headers'])
        
        return headers
    
    def _make_request_with_retry(self, payload: List[Dict], headers: Dict[str, str]) -> None:
        """
        Make HTTP request with retry logic.
        
        Args:
            payload: Request payload
            headers: HTTP headers
        """
        last_exception = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                response = requests.request(
                    method=self.method,
                    url=self.endpoint_url,
                    json=payload,
                    headers=headers,
                    timeout=30
                )
                
                response.raise_for_status()
                
                self.logger.debug(f"API request successful: {response.status_code}")
                return
                
            except requests.exceptions.RequestException as e:
                last_exception = e
                
                if attempt < self.config.max_retries:
                    delay = self._calculate_retry_delay(attempt)
                    self.logger.warning(
                        f"API request attempt {attempt + 1} failed, retrying in {delay}s: {e}"
                    )
                    time.sleep(delay)
        
        if last_exception:
            raise last_exception
    
    def _apply_rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        if not self.rate_limit:
            return
        
        min_interval = 1.0 / self.rate_limit
        elapsed = time.time() - self._last_request_time
        
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        
        self._last_request_time = time.time()