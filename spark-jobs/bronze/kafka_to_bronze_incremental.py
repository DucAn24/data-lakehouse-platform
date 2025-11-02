"""
Kafka to Bronze - INCREMENTAL BATCH Processing

Batch job với offset tracking - Chỉ xử lý data mới từ lần chạy trước.
Lưu offset vào Delta table để track progress.

Usage:
    # Load tất cả tables (incremental)
    spark-submit kafka_to_bronze_incremental.py
    
    # Load 1 table
    spark-submit kafka_to_bronze_incremental.py customers
    
    # Reset offset (load lại từ đầu)
    spark-submit kafka_to_bronze_incremental.py --reset
"""

import sys
from pathlib import Path
from datetime import datetime
import json

sys.path.append(str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from common.bronze_config import (
    KAFKA_BOOTSTRAP_SERVERS, BRONZE_PATH, TABLE_CONFIG,
    create_spark_session, create_or_update_catalog, validate_table_config
)
from utils.cdc_utils import (
    parse_cdc_events, deduplicate_records, merge_to_delta,
    print_processing_summary
)

OFFSET_PATH = 's3a://checkpoints/_offsets'

def get_last_offset(spark: SparkSession, topic: str) -> dict:

    try:
        offset_df = spark.read.format("delta").load(OFFSET_PATH)
        last_offset = offset_df.filter(f"topic = '{topic}'").orderBy(col("timestamp").desc()).first()
        
        if last_offset:
            return json.loads(last_offset.offsets)
        else:
            return None
    except:
        return None

def save_offset(spark: SparkSession, topic: str, offsets: dict):

    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    
    schema = StructType([
        StructField("topic", StringType(), False),
        StructField("offsets", StringType(), False),
        StructField("timestamp", TimestampType(), False)
    ])
    
    offset_data = [(topic, json.dumps(offsets), datetime.now())]
    offset_df = spark.createDataFrame(offset_data, schema)
    
    try:
        offset_df.write \
            .format("delta") \
            .mode("append") \
            .save(OFFSET_PATH)
    except:
        offset_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(OFFSET_PATH)

def process_table_incremental(spark: SparkSession, table_name: str, reset_offset: bool = False) -> dict:

    import time
    start_time = time.time()

    print(f"Processing: {table_name}")
    
    delta_path = f"{BRONZE_PATH}/{table_name}"
    
    try:
        config = validate_table_config(table_name)
        columns = config["columns"]
        primary_keys = config["primary_keys"]
        # Get last processed offset
        last_offset = None if reset_offset else get_last_offset(spark, table_name)
        
        if last_offset:
            starting_offsets = json.dumps(last_offset)
            print(f"Resuming from last offset: {starting_offsets}")
        else:
            starting_offsets = "earliest"
            print(f"Starting from beginning")
        
        # Read from Kafka (incremental)
        kafka_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", table_name) \
            .option("startingOffsets", starting_offsets) \
            .option("endingOffsets", "latest") \
            .load()
        
        total_messages = kafka_df.count()
        print(f"New messages: {total_messages:,}")
        
        if total_messages == 0:
            return {"table": table_name, "status": "SKIPPED", "messages": 0, "records": 0}
        
        # Get current offsets to save later
        current_offsets = {}
        for row in kafka_df.select("partition", "offset").groupBy("partition").agg(max("offset").alias("max_offset")).collect():
            current_offsets[str(row.partition)] = int(row.max_offset) + 1
        
        # Parse CDC events
        parsed_df, parsed_count = parse_cdc_events(spark, kafka_df, columns)
        
        # Deduplicate
        final_df, final_count = deduplicate_records(spark, parsed_df, primary_keys)
        print(f"Unique records: {final_count:,} (filtered {parsed_count - final_count:,})")
        
        # MERGE to Delta
        final_table_count, existing_count = merge_to_delta(
            spark, final_df, delta_path, primary_keys, columns
        )
        
        if existing_count > 0:
            print(f"MERGE completed: {final_table_count:,} total ({final_table_count - existing_count:+,})")
        else:
            print(f"Created table: {final_table_count:,} records")
        
        create_or_update_catalog(spark, table_name, delta_path)
        
        # Save offset for next run
        offset_json = {table_name: current_offsets}
        save_offset(spark, table_name, offset_json)
        print(f"Saved offset: {offset_json}")
        
        elapsed_time = time.time() - start_time
        print(f"Completed in {elapsed_time:.2f}s")
        
        return {
            "table": table_name,
            "status": "SUCCESS",
            "kafka_messages": total_messages,
            "final_records": final_table_count,
            "processing_time": elapsed_time
        }
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            "table": table_name,
            "status": "ERROR",
            "error": str(e),
            "processing_time": elapsed_time
        }

def main():
    
    reset_offset = "--reset" in sys.argv
    
    if len(sys.argv) > 1 and not sys.argv[1].startswith("--"):
        tables_to_process = [sys.argv[1]]
        if tables_to_process[0] not in TABLE_CONFIG:
            print(f"Error: Table '{tables_to_process[0]}' not found")
            print(f"Available: {', '.join(TABLE_CONFIG.keys())}")
            sys.exit(1)
    else:
        tables_to_process = list(TABLE_CONFIG.keys())
    
    print(f"Tables: {', '.join(tables_to_process)}")
    print(f"Mode: {'FULL RELOAD (reset)' if reset_offset else 'INCREMENTAL (from last offset)'}")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Bronze: {BRONZE_PATH}")
    print(f"Offset Tracking: {OFFSET_PATH}")

    spark = create_spark_session('Kafka-to-Bronze-Incremental')
    results = []
    
    for table_name in tables_to_process:
        result = process_table_incremental(spark, table_name, reset_offset)
        results.append(result)
    
    print_processing_summary(results, "INCREMENTAL BATCH PROCESSING SUMMARY")
    
    spark.stop()
    print("Processing completed\n")

if __name__ == "__main__":
    main()
