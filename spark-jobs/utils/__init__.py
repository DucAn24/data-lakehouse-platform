"""
Utility functions for Spark jobs
"""

from .cdc_utils import *

__all__ = [
    'build_cdc_schema',
    'parse_cdc_events',
    'deduplicate_records',
    'merge_to_delta',
    'get_kafka_offsets',
    'OffsetManager',
    'print_processing_summary'
]
