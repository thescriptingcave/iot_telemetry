#!/usr/bin/env python3
"""
Parquet writer utilities for IoT Telemetry
Supports ZSTD compression and optimized file writing
"""

from typing import Dict, List, Any
from pathlib import Path
import time

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    print("WARNING: pyarrow not installed. Install with: pip install pyarrow")


def create_table_from_records(records: List[Dict[str, Any]], schema_fields: Dict[str, type]) -> 'pa.Table':
    """Create PyArrow table from list of records"""
    if not PYARROW_AVAILABLE:
        raise ImportError("pyarrow is required. Install with: pip install pyarrow")
    
    if not records:
        raise ValueError("Cannot create table from empty records")
    
    columns = {}
    for field_name, field_type in schema_fields.items():
        column_data = []
        for record in records:
            value = record.get(field_name)
            if value is None:
                column_data.append(None)
            else:
                column_data.append(value)
        columns[field_name] = column_data
    
    return pa.table(columns)


def write_parquet_zstd(records: List[Dict[str, Any]], schema_fields: Dict[str, type], 
                       output_path: str, row_group_size: int = 10000) -> int:
    """
    Write records to Parquet file with ZSTD compression
    
    Args:
        records: List of record dictionaries
        schema_fields: Dict mapping field names to Python types
        output_path: Path to write Parquet file
        row_group_size: Number of rows per row group (default 10000)
    
    Returns:
        Number of records written
    """
    if not PYARROW_AVAILABLE:
        raise ImportError("pyarrow is required. Install with: pip install pyarrow")
    
    if not records:
        return 0
    
    table = create_table_from_records(records, schema_fields)
    
    pq.write_table(
        table,
        output_path,
        compression='zstd',
        compression_level=3,
        row_group_size=row_group_size,
        use_dictionary=True
    )
    
    return len(records)


def write_partitioned_parquet_zstd(
    records_by_partition: Dict[str, List[Dict[str, Any]]],
    schema_fields: Dict[str, type],
    base_path: str,
    partition_cols: List[str],
    row_group_size: int = 10000
) -> Dict[str, int]:
    """
    Write records partitioned by column values
    
    Args:
        records_by_partition: Dict mapping partition path to records
        schema_fields: Dict mapping field names to Python types
        base_path: Base directory for partitioned data
        partition_cols: List of partition column names
        row_group_size: Number of rows per row group
    
    Returns:
        Dict mapping partition path to record count
    """
    if not PYARROW_AVAILABLE:
        raise ImportError("pyarrow is required. Install with: pip install pyarrow")
    
    counts = {}
    
    for partition_path, records in records_by_partition.items():
        if not records:
            continue
        
        full_path = Path(base_path) / partition_path
        full_path.mkdir(parents=True, exist_ok=True)
        
        filename = f"data_{int(time.time())}_{len(counts):06d}.parquet"
        output_file = str(full_path / filename)
        
        count = write_parquet_zstd(records, schema_fields, output_file, row_group_size)
        counts[partition_path] = count
    
    return counts


def get_schema_fields_from_avro_schema(avro_schema: Dict[str, Any]) -> Dict[str, type]:
    """
    Convert Avro schema to PyArrow field types
    Note: This is a simplified conversion; may need customization
    """
    type_mapping = {
        'int': int,
        'long': int,
        'float': float,
        'double': float,
        'boolean': bool,
        'string': str,
        'bytes': bytes,
        'null': type(None)
    }
    
    fields = {}
    for field_def in avro_schema.get('fields', []):
        field_name = field_def['name']
        field_type = field_def['type']
        
        if isinstance(field_type, str):
            fields[field_name] = type_mapping.get(field_type, str)
        elif isinstance(field_type, dict):
            if field_type.get('type') == 'enum':
                fields[field_name] = str
            elif field_type.get('type') == 'record':
                fields[field_name] = str
            else:
                fields[field_name] = str
        elif isinstance(field_type, list):
            non_null_types = [t for t in field_type if t != 'null']
            if non_null_types:
                if isinstance(non_null_types[0], str):
                    fields[field_name] = type_mapping.get(non_null_types[0], str)
                else:
                    fields[field_name] = str
            else:
                fields[field_name] = type(None)
    
    return fields