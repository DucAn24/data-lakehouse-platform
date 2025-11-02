"""
Common configurations for all Spark jobs
"""

from .bronze_config import *

__all__ = [
    'KAFKA_BOOTSTRAP_SERVERS',
    'BRONZE_PATH',
    'TABLE_CONFIG',
    'create_spark_session',
    'create_or_update_catalog',
    'validate_table_config'
]
