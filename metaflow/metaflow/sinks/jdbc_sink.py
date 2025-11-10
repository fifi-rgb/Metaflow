"""
JDBC sink for writing to relational databases.

Supports:
- PostgreSQL, MySQL, Oracle, SQL Server
- Batch inserts
- Parallel writes
- Transaction management
"""

from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, TimestampType, DateType, DecimalType

from metaflow.sinks.base_sink import BaseSink, SinkConfig, WriteMode


class JdbcSink(BaseSink):
    """
    Sink for writing data to JDBC-compatible databases.
    
    Supports:
    - All major relational databases
    - Batch inserts for performance
    - Parallel writes with partitioning
    - Transaction control
    """
    
    def __init__(
        self,
        spark,
        config: SinkConfig,
        jdbc_url: str,
        table: str,
        properties: Optional[Dict[str, str]] = None
    ):
        """
        Initialize JDBC sink.
        
        Args:
            spark: Active SparkSession
            config: Sink configuration
            jdbc_url: JDBC connection URL
            table: Target table name
            properties: JDBC connection properties
        """
        super().__init__(spark, config)
        self.jdbc_url = jdbc_url
        self.table = table
        self.properties = properties or {}
        
        # Set defaults
        self.properties.setdefault('driver', self._detect_driver(jdbc_url))
    
    def _detect_driver(self, jdbc_url: str) -> str:
        """
        Detect JDBC driver from URL.
        
        Args:
            jdbc_url: JDBC URL
            
        Returns:
            Driver class name
        """
        drivers = {
            'postgresql': 'org.postgresql.Driver',
            'mysql': 'com.mysql.cj.jdbc.Driver',
            'oracle': 'oracle.jdbc.driver.OracleDriver',
            'sqlserver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            'db2': 'com.ibm.db2.jcc.DB2Driver',
        }
        
        for db_type, driver in drivers.items():
            if db_type in jdbc_url.lower():
                return driver
        
        return 'org.postgresql.Driver'  # Default
    
    def _do_write(self, df: DataFrame, **kwargs) -> None:
        """
        Write DataFrame to JDBC table.
        
        Args:
            df: DataFrame to write
            **kwargs: Additional JDBC options
        """
        writer = df.write.format('jdbc') \
            .option('url', self.jdbc_url) \
            .option('dbtable', self.table) \
            .mode(self._get_jdbc_mode())
        
        # Apply connection properties
        for key, value in self.properties.items():
            writer = writer.option(key, value)
        
        # Batch size for inserts
        writer = writer.option('batchsize', str(self.config.batch_size))
        
        # Parallel writes
        if 'numPartitions' in kwargs:
            writer = writer.option('numPartitions', str(kwargs['numPartitions']))
        
        # Isolation level
        if 'isolationLevel' in kwargs:
            writer = writer.option('isolationLevel', kwargs['isolationLevel'])
        
        # Truncate option
        if kwargs.get('truncate', False):
            writer = writer.option('truncate', 'true')
        
        # Create table options
        if 'createTableOptions' in kwargs:
            writer = writer.option('createTableOptions', kwargs['createTableOptions'])
        
        # Custom options
        for key, value in self.config.custom_options.items():
            writer = writer.option(key, value)
        
        writer.save()
        
        self.logger.info(f"Wrote data to JDBC table: {self.table}")
    
    def _get_jdbc_mode(self) -> str:
        """
        Convert WriteMode to JDBC save mode.
        
        Returns:
            JDBC save mode string
        """
        mode_mapping = {
            WriteMode.APPEND: 'append',
            WriteMode.OVERWRITE: 'overwrite',
            WriteMode.ERROR_IF_EXISTS: 'error',
            WriteMode.IGNORE: 'ignore',
        }
        
        return mode_mapping.get(self.config.write_mode, 'append')
    
    def execute_query(self, query: str) -> None:
        """
        Execute arbitrary SQL query.
        
        Args:
            query: SQL query to execute
        """
        import jaydebeapi
        
        conn = jaydebeapi.connect(
            self.properties['driver'],
            self.jdbc_url,
            [self.properties.get('user', ''), self.properties.get('password', '')],
        )
        
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            self.logger.info(f"Executed query: {query[:100]}...")
        finally:
            conn.close()
    
    def create_table_from_schema(self, df: DataFrame, **kwargs) -> None:
        """
        Create table from DataFrame schema.
        
        Args:
            df: DataFrame with desired schema
            **kwargs: Additional CREATE TABLE options
        """
        type_mapping = {
            StringType(): 'VARCHAR(255)',
            IntegerType(): 'INTEGER',
            LongType(): 'BIGINT',
            DoubleType(): 'DOUBLE PRECISION',
            FloatType(): 'FLOAT',
            BooleanType(): 'BOOLEAN',
            TimestampType(): 'TIMESTAMP',
            DateType(): 'DATE',
            DecimalType(): 'DECIMAL(38,18)',
        }
        
        columns = []
        for field in df.schema.fields:
            col_type = type_mapping.get(type(field.dataType), 'VARCHAR(255)')
            nullable = 'NULL' if field.nullable else 'NOT NULL'
            columns.append(f"{field.name} {col_type} {nullable}")
        
        primary_key = kwargs.get('primary_key')
        if primary_key:
            columns.append(f"PRIMARY KEY ({', '.join(primary_key)})")
        
        create_query = f"CREATE TABLE IF NOT EXISTS {self.table} ({', '.join(columns)})"
        
        self.execute_query(create_query)
        self.logger.info(f"Created table: {self.table}")