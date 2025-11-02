from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Tuple
import json
from datetime import datetime

def build_cdc_schema(columns: list) -> str:

    struct_schema = ", ".join([f"{col}:STRING" for col in columns])
    
    return f"""
        before STRUCT<{struct_schema}>, 
        after STRUCT<{struct_schema}>, 
        op STRING, 
        ts_ms BIGINT,
        source STRUCT<
            version:STRING,
            connector:STRING,
            name:STRING,
            ts_ms:BIGINT,
            snapshot:STRING,
            db:STRING,
            schema:STRING,
            table:STRING,
            txId:BIGINT,
            lsn:BIGINT,
            xmin:BIGINT
        >
    """

def parse_cdc_events(spark: SparkSession, kafka_df: DataFrame, columns: list, 
                     view_name: str = "kafka_data") -> Tuple[DataFrame, int]:
    """
    Parse CDC events from Kafka DataFrame
    
    Returns:
        Tuple of (parsed_df, count)
    """
    kafka_df.createOrReplaceTempView(view_name)
    
    cdc_schema = build_cdc_schema(columns)
    
    data_columns = ', '.join([
        f"COALESCE(cdc_data.after.{col}, cdc_data.before.{col}) AS {col}" 
        for col in columns
    ])
    
    parsed_sql = f"""
    SELECT
        {data_columns},
        CASE cdc_data.op
            WHEN 'r' THEN 'READ'
            WHEN 'c' THEN 'CREATE'
            WHEN 'u' THEN 'UPDATE'
            WHEN 'd' THEN 'DELETE'
            ELSE 'UNKNOWN'
        END AS operation_type,
        cdc_data.op AS cdc_operation,
        cdc_data.ts_ms AS cdc_timestamp_ms,
        CURRENT_TIMESTAMP() AS processed_at,
        CASE WHEN cdc_data.op = 'd' THEN true ELSE false END AS is_deleted,
        topic AS kafka_topic,
        partition AS kafka_partition,
        offset AS kafka_offset
    FROM (
        SELECT
            from_json(CAST(value AS STRING), '{cdc_schema}') AS cdc_data,
            topic,
            partition,
            offset
        FROM {view_name}
    )
    WHERE cdc_data IS NOT NULL
    """
    
    parsed_df = spark.sql(parsed_sql)
    count = parsed_df.count()
    
    return parsed_df, count

def deduplicate_records(spark: SparkSession, parsed_df: DataFrame, 
                       primary_keys: list, filter_deletes: bool = True) -> Tuple[DataFrame, int]:
    """
    Deduplicate records keeping the latest version
    
    Args:
        spark: SparkSession
        parsed_df: Parsed CDC DataFrame
        primary_keys: List of primary key columns
        filter_deletes: If True, filter out DELETE operations
        
    Returns:
        Tuple of (deduped_df, count)
    """
    parsed_df.createOrReplaceTempView("parsed_cdc")
    pk_columns = ", ".join(primary_keys)
    
    dedup_sql = f"""
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {pk_columns}
                ORDER BY cdc_timestamp_ms DESC, kafka_offset DESC
            ) AS row_num
        FROM parsed_cdc
    )
    WHERE row_num = 1
    """
    
    deduped_df = spark.sql(dedup_sql)
    
    if filter_deletes:
        deduped_df = deduped_df.filter("is_deleted = false")
    
    final_df = deduped_df.drop("is_deleted", "row_num")
    count = final_df.count()
    
    return final_df, count

def merge_to_delta(spark: SparkSession, source_df: DataFrame, delta_path: str,
                   primary_keys: list, columns: list) -> Tuple[int, int]:
    """
    MERGE source data into Delta table
    
    Returns:
        Tuple of (final_count, existing_count)
    """
    # Check if table exists
    try:
        existing_df = spark.read.format("delta").load(delta_path)
        table_exists = True
        existing_count = existing_df.count()
    except:
        table_exists = False
        existing_count = 0
    
    if table_exists:
        source_df.createOrReplaceTempView("source_data")
        existing_df.createOrReplaceTempView("target_table")
        
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
        
        all_columns = columns + ["operation_type", "cdc_operation", "cdc_timestamp_ms", 
                                "processed_at", "kafka_topic", "kafka_partition", "kafka_offset"]
        insert_cols = ", ".join(all_columns)
        insert_vals = ", ".join([f"source.{col}" for col in all_columns])
        
        merge_sql = f"""
        MERGE INTO delta.`{delta_path}` AS target
        USING source_data AS source
        ON {merge_condition}
        WHEN MATCHED AND source.cdc_timestamp_ms >= target.cdc_timestamp_ms THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
        """
        
        spark.sql(merge_sql)
        final_count = spark.read.format("delta").load(delta_path).count()
    else:
        # Create new table
        source_df.write.format("delta").mode("overwrite").save(delta_path)
        final_count = source_df.count()
    
    return final_count, existing_count

def get_kafka_offsets(df: DataFrame) -> dict:

    offsets = {}
    for row in df.select("partition", "offset") \
                 .groupBy("partition") \
                 .agg({"offset": "max"}) \
                 .collect():
        offsets[str(row.partition)] = int(row['max(offset)']) + 1
    return offsets

class OffsetManager:
    
    def __init__(self, spark: SparkSession, offset_path: str):
        self.spark = spark
        self.offset_path = offset_path
    
    def get_last_offset(self, topic: str) -> dict:

        try:
            from pyspark.sql.functions import col
            offset_df = self.spark.read.format("delta").load(self.offset_path)
            last_offset = offset_df.filter(f"topic = '{topic}'") \
                                   .orderBy(col("timestamp").desc()) \
                                   .first()
            
            if last_offset:
                return json.loads(last_offset.offsets)
            else:
                return None
        except:
            return None
    
    def save_offset(self, topic: str, offsets: dict):

        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        
        schema = StructType([
            StructField("topic", StringType(), False),
            StructField("offsets", StringType(), False),
            StructField("timestamp", TimestampType(), False)
        ])
        
        offset_data = [(topic, json.dumps(offsets), datetime.now())]
        offset_df = self.spark.createDataFrame(offset_data, schema)
        
        try:
            offset_df.write.format("delta").mode("append").save(self.offset_path)
        except:
            offset_df.write.format("delta").mode("overwrite").save(self.offset_path)

def print_processing_summary(results: list, title: str = "BATCH PROCESSING SUMMARY"):

    print("\n" + "="*100)
    print(title)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*100)
    
    print(f"\n{'Table':<40} {'Status':<12} {'Messages':<12} {'Records':<12} {'Time (s)':<10}")
    print("-"*100)
    
    total_messages = 0
    total_records = 0
    total_time = 0
    success_count = 0
    
    for result in results:
        table = result["table"]
        status = result["status"]
        
        if status == "SUCCESS":
            kafka_msg = result.get("kafka_messages", 0)
            delta_rec = result.get("final_records", 0)
            time_sec = result.get("processing_time", 0)
            
            total_messages += kafka_msg
            total_records += delta_rec
            total_time += time_sec
            success_count += 1
            
            print(f"{table:<40} {status:<12} {kafka_msg:<12,} {delta_rec:<12,} {time_sec:<10.2f}")
            
        elif status == "SKIPPED":
            print(f"{table:<40} {status:<12} {'0':<12} {'N/A':<12} {'N/A':<10}")
        else:
            error = result.get("error", "Unknown")
            print(f"{table:<40} {status:<12} {'N/A':<12} {'N/A':<12} {'N/A':<10}")
            print(f"  Error: {error}")
    
    print("-"*100)
    print(f"\nTOTALS:")
    print(f"  Tables Processed: {len(results)}")
    print(f"  Successful: {success_count}")
    print(f"  Failed: {len(results) - success_count}")
    print(f"  Total Messages: {total_messages:,}")
    print(f"  Total Records: {total_records:,}")
    if total_time > 0:
        print(f"  Total Time: {total_time:.2f}s")
    print("="*100 + "\n")
