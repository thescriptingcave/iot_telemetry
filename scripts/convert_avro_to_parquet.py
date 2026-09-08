#!/usr/bin/env python3
"""
Convert Avro files to Parquet with ZSTD compression
Usage: python convert_avro_to_parquet.py <input_dir> <output_dir>
"""

import sys
import json
import os
from pathlib import Path
from datetime import datetime

try:
    import fastavro
    from fastavro import reader as avro_reader
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError as e:
    print(f"Error: {e}")
    print("Install dependencies: pip install pyarrow fastavro")
    sys.exit(1)


def get_schema_fields(avro_schema: dict) -> dict:
    """Extract field types from Avro schema"""
    type_mapping = {
        'int': int,
        'long': int,
        'float': float,
        'double': float,
        'boolean': bool,
        'string': str,
        'bytes': bytes,
    }
    
    fields = {}
    for field_def in avro_schema.get('fields', []):
        field_name = field_def['name']
        field_type = field_def['type']
        
        if isinstance(field_type, str):
            if field_type in type_mapping:
                fields[field_name] = type_mapping[field_type]
            elif field_type == 'timestamp-millis':
                fields[field_name] = datetime
            else:
                fields[field_name] = str
        elif isinstance(field_type, dict):
            if field_type.get('type') == 'enum':
                fields[field_name] = str
            elif field_type.get('type') == 'record':
                fields[field_name] = str
            elif field_type.get('logicalType') == 'timestamp-millis':
                fields[field_name] = datetime
            else:
                fields[field_name] = str
        elif isinstance(field_type, list):
            non_null_types = [t for t in field_type if t != 'null']
            if non_null_types:
                if isinstance(non_null_types[0], str):
                    if non_null_types[0] in type_mapping:
                        fields[field_name] = type_mapping[non_null_types[0]]
                    elif non_null_types[0] == 'timestamp-millis':
                        fields[field_name] = datetime
                    else:
                        fields[field_name] = str
                elif isinstance(non_null_types[0], dict):
                    if non_null_types[0].get('logicalType') == 'timestamp-millis':
                        fields[field_name] = datetime
                    else:
                        fields[field_name] = str
            else:
                fields[field_name] = type(None)
    
    return fields


def convert_avro_to_parquet(avro_path: str, parquet_path: str, schema_fields: dict) -> int:
    """Convert single Avro file to Parquet"""
    records = []
    
    with open(avro_path, 'rb') as f:
        avro_reader_obj = avro_reader(f)
        for record in avro_reader_obj:
            records.append(record)
    
    if not records:
        return 0
    
    table = pa.table(records)
    
    pq.write_table(
        table,
        parquet_path,
        compression='zstd',
        compression_level=3,
        row_group_size=10000,
        use_dictionary=True
    )
    
    return len(records)


def convert_directory(input_dir: str, output_dir: str):
    """Recursively convert all Avro files in directory to Parquet"""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    if not input_path.exists():
        print(f"Error: Input directory does not exist: {input_dir}")
        sys.exit(1)
    
    output_path.mkdir(parents=True, exist_ok=True)
    
    avro_files = list(input_path.glob('**/*.avro'))
    total_records = 0
    total_files = 0
    
    print(f"Found {len(avro_files)} Avro files to convert")
    print(f"Input: {input_dir}")
    print(f"Output: {output_dir}")
    print("-" * 60)
    
    for avro_file in avro_files:
        relative_path = avro_file.relative_to(input_path)
        parquet_file = output_path / relative_path.with_suffix('.parquet')
        parquet_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            schema_fields = get_schema_fields({'fields': []})
            count = convert_avro_to_parquet(str(avro_file), str(parquet_file), schema_fields)
            total_records += count
            total_files += 1
            
            input_size = avro_file.stat().st_size
            output_size = parquet_file.stat().st_size
            compression_ratio = (1 - output_size / input_size) * 100 if input_size > 0 else 0
            
            print(f"[{total_files}] {relative_path}")
            print(f"      Records: {count:,} | Size: {input_size:,} -> {output_size:,} bytes ({compression_ratio:.1f}% reduction)")
        except Exception as e:
            print(f"      ERROR: {e}")
    
    print("-" * 60)
    print(f"Conversion complete!")
    print(f"  Files: {total_files}")
    print(f"  Records: {total_records:,}")


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python convert_avro_to_parquet.py <input_dir> <output_dir>")
        print("Example: python convert_avro_to_parquet.py ./data/avro_incoming ./data/parquet_converted")
        sys.exit(1)
    
    convert_directory(sys.argv[1], sys.argv[2])