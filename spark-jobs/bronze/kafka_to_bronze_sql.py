"""
Kafka to Bronze - CDC Streaming Processor

Xử lý CDC events từ Kafka và MERGE vào Delta Lake.
- 'c' (CREATE)  → INSERT
- 'u' (UPDATE)  → UPDATE  
- 'd' (DELETE)  → DELETE (hard delete)
- 'r' (READ)    → Snapshot

NOTE: is_deleted flag chỉ dùng trong MERGE logic, không lưu vào table.
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession

from common.bronze_config import (
    KAFKA_BOOTSTRAP_SERVERS, BRONZE_PATH, TABLE_CONFIG,
    create_spark_session, create_or_update_catalog, validate_table_config
)
from utils.cdc_utils import (
    build_cdc_schema, parse_cdc_events, deduplicate_records
)

# Streaming-specific configs
CHECKPOINT_PATH = 's3a://checkpoints/bronze-unified'
TRIGGER_INTERVAL = 600
MAX_OFFSETS_PER_TRIGGER = 500
STARTING_OFFSETS = 'earliest'
ENABLE_MEMORY_OPTIMIZATION = True

def process_cdc_batch(batch_df, batch_id: int, table_name: str, delta_path: str):
    """
    Process CDC batch for a single table: Parse → Deduplicate → MERGE to Delta Lake
    NOTE: is_deleted flag chỉ dùng trong MERGE, không lưu vào table
    """
    import time
    start_time = time.time()
    
    try:
        if batch_df.isEmpty():
            print(f"[Batch {batch_id}] {table_name}: Empty batch, skipping")
            return
        
        spark = batch_df.sparkSession
        
        record_count = batch_df.count()
        print(f"[Batch {batch_id}] {table_name}: Processing {record_count} records")
        
        config = validate_table_config(table_name)
        columns = config["columns"]
        primary_keys = config["primary_keys"]
        
        # Parse CDC events using shared utility
        parsed_df, parsed_count = parse_cdc_events(spark, batch_df, columns, "cdc_batch")
        print(f"[Batch {batch_id}] {table_name}: Parsed {parsed_count} CDC messages")
        
        # Deduplicate within batch using shared utility (don't filter deletes yet for streaming)
        deduped_df, final_count = deduplicate_records(spark, parsed_df, primary_keys, filter_deletes=False)
        deduped_df.createOrReplaceTempView("source_data")
        print(f"[Batch {batch_id}] {table_name}: After dedup: {final_count} records (unique in batch)")
        
        # Check if Delta table exists
        try:
            spark.sql(f"SELECT 1 FROM delta.`{delta_path}` LIMIT 1")
            table_exists = True
        except:
            table_exists = False
        
        if table_exists:
            spark.read.format("delta").load(delta_path).createOrReplaceTempView("target_table")
            
            # Build MERGE components
            merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])
            
            update_columns = [f"{col} = source.{col}" for col in columns]
            update_columns.extend([
                "operation_type = source.operation_type",
                "cdc_operation = source.cdc_operation", 
                "cdc_timestamp_ms = source.cdc_timestamp_ms",
                "processed_at = source.processed_at",
                "kafka_topic = source.kafka_topic",
                "kafka_partition = source.kafka_partition",
                "kafka_offset = source.kafka_offset"
            ])
            set_clause = ", ".join(update_columns)
            
            all_columns = columns + ["operation_type", "cdc_operation", "cdc_timestamp_ms", "processed_at", 
                                    "kafka_topic", "kafka_partition", "kafka_offset"]
            insert_cols = ", ".join(all_columns)
            insert_vals = ", ".join([f"source.{col}" for col in all_columns])
            
            # Execute MERGE with condition to only UPDATE if source is NEWER
            # FIX: Add timestamp comparison to prevent overwriting newer data with older data
            merge_sql = f"""
            MERGE INTO delta.`{delta_path}` AS target
            USING source_data AS source
            ON {merge_condition}
            WHEN MATCHED AND source.is_deleted = true THEN
                DELETE
            WHEN MATCHED AND source.is_deleted = false AND source.cdc_timestamp_ms >= target.cdc_timestamp_ms THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED AND source.is_deleted = false THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals})
            """
            
            merge_result = spark.sql(merge_sql)
            
            # Get merge metrics
            try:
                # Count current total
                current_count = spark.read.format("delta").load(delta_path).count()
                print(f"[Batch {batch_id}] {table_name}: MERGE completed | Total records in table: {current_count:,}")
            except:
                print(f"[Batch {batch_id}] {table_name}: MERGE completed")
            
            try:
                spark.sql(f"REFRESH TABLE bronze.{table_name}")
            except:
                pass
            
        else:
            # Create new Delta table (drop is_deleted column before saving)
            # Filter out DELETE operations for initial load
            final_df = deduped_df.filter("is_deleted = false").drop("is_deleted", "row_num")
            
            initial_count = final_df.count()
            
            final_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(delta_path)
            
            print(f"[Batch {batch_id}] {table_name}: Created Delta table with {initial_count} records (filtered from {final_count} in batch)")
            create_or_update_catalog(spark, table_name, delta_path)

    except Exception as e:
        print(f"[Batch {batch_id}] {table_name}: ERROR in batch processing: {e}")
        import traceback
        traceback.print_exc()
        

def process_multi_table_batch(batch_df, batch_id: int):
    """
    Process multiple tables in a single batch with memory management
    """
    import time
    batch_start = time.time()
    
    if batch_df.isEmpty():
        print(f"[Batch {batch_id}] Empty batch, skipping")
        return
    
    spark = batch_df.sparkSession
    
    # Group by topic (topic = table name)
    topics = batch_df.select("topic").distinct().collect()
    total_records = batch_df.count()
    
    print(f"\n{'='*80}")
    print(f"[Batch {batch_id}] Processing {len(topics)} topics | Total records: {total_records}")
    print(f"{'='*80}")
    
    for row in topics:
        table_name = row.topic
        
        # Filter data for this table
        table_df = batch_df.filter(f"topic = '{table_name}'")
        delta_path = f"{BRONZE_PATH}/{table_name}"
        
        # Process this table's data
        process_cdc_batch(table_df, batch_id, table_name, delta_path)
        
        # Force garbage collection between tables to free memory
        if ENABLE_MEMORY_OPTIMIZATION:
            import gc
            gc.collect()
    
    batch_elapsed = time.time() - batch_start
    print(f"[Batch {batch_id}] ✅ Completed all {len(topics)} tables in {batch_elapsed:.2f}s\n")

def start_unified_streaming(spark: SparkSession):
    """Start unified streaming query for all tables"""
    all_tables = list(TABLE_CONFIG.keys())
    topics = ",".join(all_tables)
    
    print(f"📡 Subscribing to topics: {topics}")
    
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topics) \
        .option("startingOffsets", STARTING_OFFSETS) \
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER) \
        .option("failOnDataLoss", "false") \
        .load()
    
    query = kafka_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .foreachBatch(process_multi_table_batch) \
        .trigger(processingTime=f'{TRIGGER_INTERVAL} seconds') \
        .start()
    
    return query

def main():
    all_tables = list(TABLE_CONFIG.keys())
    
    print("\n" + "="*80)
    print("KAFKA TO BRONZE - CDC STREAMING JOB")
    print("="*80)
    print(f"Tables: {', '.join(all_tables)}")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Bronze: {BRONZE_PATH}")
    print(f"Trigger Interval: {TRIGGER_INTERVAL}s")
    print(f"Max Offsets/Trigger: {MAX_OFFSETS_PER_TRIGGER}")
    print("="*80)
    
    spark = create_spark_session('Kafka-to-Bronze-Streaming', streaming=True)
    
    query = start_unified_streaming(spark)
    print(f"Streaming query started for {len(all_tables)} tables")
    print("Processing CDC events... (Press Ctrl+C to stop)\n")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming...")
        query.stop()
        print("Stream stopped gracefully")

if __name__ == "__main__":
    main()
