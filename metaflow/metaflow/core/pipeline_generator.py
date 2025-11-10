"""Generate PySpark pipeline code."""

from pathlib import Path
from typing import Dict, List, Any
from jinja2 import Template
from metaflow.core.metadata_extractor import MetadataExtractor, TableMetadata
from metaflow.core.schema_generator import SchemaGenerator


class PipelineGenerator:
    """Generate PySpark pipeline code from configuration."""
    
    BATCH_TEMPLATE = '''"""
Auto-generated batch pipeline by MetaFlow.
Generated on: {{ timestamp }}
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, current_timestamp
from pyspark.sql.types import *
from delta.tables import DeltaTable

# Initialize Spark
spark = SparkSession.builder \\
    .appName("MetaFlow - {{ config.source.database }}") \\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \\
    .getOrCreate()

{% for table in tables %}
def process_{{ table.name.lower() }}():
    """Process table: {{ table.name }}"""
    
    # Define schema
    schema = {{ table.schema_code }}
    
    # Read from source
    df = spark.read \\
        .format("jdbc") \\
        .option("url", "{{ config.source.connection_string }}") \\
        .option("dbtable", "{{ table.name }}") \\
        .option("driver", "{{ driver_class }}") \\
        .load()
    
    {% if config.pipeline.deduplicate and table.primary_keys %}
    # Deduplicate by primary key
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    window = Window.partitionBy({{ table.pk_list }}).orderBy(col("updated_at").desc())
    df = df.withColumn("_rn", row_number().over(window)) \\
        .filter(col("_rn") == 1) \\
        .drop("_rn")
    {% endif %}
    
    # Add partition column
    {% if config.target.partition_by == "created_year" %}
    df = df.withColumn("created_year", year(col("created_at")))
    {% endif %}
    
    # Write to Delta
    target_path = "{{ config.target.path }}/{{ table.name }}"
    
    {% if config.pipeline.merge_mode == "upsert" and table.primary_keys %}
    # Upsert mode
    if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forPath(spark, target_path)
        
        delta_table.alias("target").merge(
            df.alias("source"),
            "{{ table.merge_condition }}"
        ).whenMatchedUpdateAll() \\
         .whenNotMatchedInsertAll() \\
         .execute()
    else:
        # Initial write
        df.write \\
            .format("delta") \\
            .mode("overwrite") \\
            {% if config.target.partition_by %}
            .partitionBy("{{ config.target.partition_by }}") \\
            {% endif %}
            .save(target_path)
    {% else %}
    # Overwrite mode
    df.write \\
        .format("delta") \\
        .mode("overwrite") \\
        {% if config.target.partition_by %}
        .partitionBy("{{ config.target.partition_by }}") \\
        {% endif %}
        .save(target_path)
    {% endif %}
    
    {% if config.target.optimize %}
    # Optimize table
    spark.sql(f"OPTIMIZE delta.`{target_path}`")
    {% endif %}
    
    print(f"✓ Processed {{ table.name }}: {df.count()} records")

{% endfor %}

if __name__ == "__main__":
    print("Starting MetaFlow batch pipeline...")
    
    {% for table in tables %}
    process_{{ table.name.lower() }}()
    {% endfor %}
    
    print("✓ Pipeline completed successfully")
'''
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize pipeline generator.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.extractor = MetadataExtractor(config["source"]["connection_string"])
        self.schema_generator = SchemaGenerator(self.extractor.db_type)
    
    def generate(self, output_dir: str) -> List[str]:
        """
        Generate pipeline files.
        
        Args:
            output_dir: Directory to write generated files
            
        Returns:
            List of generated file paths
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Extract metadata
        table_names = self.config["source"].get("tables")
        metadata = self.extractor.extract(table_names)
        
        # Prepare template context
        tables = []
        for table in metadata.tables:
            schema_code = self.schema_generator.generate_struct_type(table)
            merge_condition = self.schema_generator.generate_primary_key_condition(table)
            pk_list = ", ".join(f'"{pk}"' for pk in table.primary_keys)
            
            tables.append({
                "name": table.name,
                "schema_code": schema_code,
                "merge_condition": merge_condition,
                "pk_list": pk_list,
                "primary_keys": table.primary_keys,
            })
        
        context = {
            "config": self.config,
            "tables": tables,
            "timestamp": "2025-01-01",  # Use actual timestamp
            "driver_class": self._get_driver_class(),
        }
        
        # Generate batch pipeline
        template = Template(self.BATCH_TEMPLATE)
        batch_code = template.render(**context)
        
        batch_file = output_path / "batch_pipeline.py"
        batch_file.write_text(batch_code)
        
        # Generate streaming pipeline (simplified for now)
        stream_file = output_path / "streaming_pipeline.py"
        stream_file.write_text("# Streaming pipeline - coming soon")
        
        return [str(batch_file), str(stream_file)]
    
    def _get_driver_class(self) -> str:
        """Get JDBC driver class name."""
        db_type = self.extractor.db_type
        
        drivers = {
            "postgresql": "org.postgresql.Driver",
            "mysql": "com.mysql.cj.jdbc.Driver",
            "oracle": "oracle.jdbc.driver.OracleDriver",
            "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }
        
        return drivers.get(db_type, "org.postgresql.Driver")


class BatchPipeline:
    """Execute batch pipeline."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    def run(self):
        """Run the batch pipeline."""
        print("Running batch pipeline...")
        # Implementation would import and execute generated code
        

class StreamingPipeline:
    """Execute streaming pipeline."""
    
    def __init__(self, config: Dict[str, Any], checkpoint: str):
        self.config = config
        self.checkpoint = checkpoint
    
    def run(self):
        """Run the streaming pipeline."""
        print("Running streaming pipeline...")
        # Implementation would start streaming jobs