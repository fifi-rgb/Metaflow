"""
MetaFlow - Metadata-Driven ETL Pipelines for Apache Spark & Delta Lake

This package provides tools for automatically generating data pipelines
based on database metadata.
"""

__version__ = "0.1.0"
__author__ = "Wan Ying Chau"
__email__ = "chau.wan_ying@edu.escp.eu"

from metaflow.core.metadata_extractor import MetadataExtractor
from metaflow.core.pipeline_generator import PipelineGenerator
from metaflow.core.schema_generator import SchemaGenerator

__all__ = [
    "MetadataExtractor",
    "PipelineGenerator",
    "SchemaGenerator",
]