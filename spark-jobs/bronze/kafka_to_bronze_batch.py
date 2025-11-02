"""
Kafka to Bronze - BATCH Processing (One-time Load)

Usage:
    spark-submit kafka_to_bronze_batch.py [table_name]
    
    # Load all tables
    spark-submit kafka_to_bronze_batch.py
    
    # Load 1 table
    spark-submit kafka_to_bronze_batch.py customers
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

def save_offset(spark: SparkSession, topic: str, offsets: dict):
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
        print(f"Offset saved to: {OFFSET_PATH}")
    except:
        # First time - create table
        offset_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(OFFSET_PATH)
        print(f"Offset table created at: {OFFSET_PATH}")

def process_table_batch(spark, table_name: str) -> dict:

    import time
    start_time = time.time()
    
    print(f"Processing: {table_name}")

    delta_path = f"{BRONZE_PATH}/{table_name}"
    
    try:
        config = validate_table_config(table_name)
        columns = config["columns"]
        primary_keys = config["primary_keys"]
        
        # Read from Kafka
        kafka_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", table_name) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        total_messages = kafka_df.count()
        print(f"Kafka messages: {total_messages:,}")
        
        if total_messages == 0:
            return {"table": table_name, "status": "SKIPPED", "messages": 0, "records": 0}
        
        # Get current offsets to save later
        current_offsets = {}
        for row in kafka_df.select("partition", "offset") \
                           .groupBy("partition") \
                           .agg(max("offset").alias("max_offset")) \
                           .collect():
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
        
        # Save offset for incremental jobs to resume
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
    
    if len(sys.argv) > 1:
        tables_to_process = [sys.argv[1]]
        if tables_to_process[0] not in TABLE_CONFIG:
            print(f"Error: Table '{tables_to_process[0]}' not found in config")
            print(f"Available tables: {', '.join(TABLE_CONFIG.keys())}")
            sys.exit(1)
    else:
        tables_to_process = list(TABLE_CONFIG.keys())
    

    print(f"Tables: {', '.join(tables_to_process)}")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Bronze: {BRONZE_PATH}")
    print(f"Offset Path: {OFFSET_PATH}")

    spark = create_spark_session('Kafka-to-Bronze-Batch')
    
    results = []
    for table_name in tables_to_process:
        result = process_table_batch(spark, table_name)
        results.append(result)
    
    print_processing_summary(results, "BATCH PROCESSING SUMMARY")
    
    spark.stop()
    print("Batch processing completed\n")

if __name__ == "__main__":
    main()
