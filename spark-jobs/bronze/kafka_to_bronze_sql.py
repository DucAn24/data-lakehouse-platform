"""
Kafka to Bronze - CDC Streaming Processor (SQL-based)

Xử lý CDC events từ Kafka và MERGE vào Delta Lake.
- 'c' (CREATE)  → INSERT
- 'u' (UPDATE)  → UPDATE  
- 'd' (DELETE)  → DELETE (hard delete)
- 'r' (READ)    → Snapshot

NOTE: is_deleted flag chỉ dùng trong MERGE logic, không lưu vào table.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'admin'
S3_SECRET_KEY = 'password123'
BRONZE_PATH = 's3a://bronze'
CHECKPOINT_PATH = 's3a://checkpoints'
HIVE_METASTORE_URIS = 'thrift://hive-metastore:9083'
TRIGGER_INTERVAL = 30
MAX_OFFSETS_PER_TRIGGER = 10000
STARTING_OFFSETS = 'earliest'

TABLE_CONFIG = {
    "customers": {
        "primary_keys": ["customer_id"],
        "columns": ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]
    },
    "sellers": {
        "primary_keys": ["seller_id"],
        "columns": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]
    },
    "products": {
        "primary_keys": ["product_id"],
        "columns": ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght", 
                   "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
    },
    "orders": {
        "primary_keys": ["order_id"],
        "columns": ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
                   "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]
    },
    "order_items": {
        "primary_keys": ["order_id", "order_item_id"],
        "columns": ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"]
    },
    "order_payments": {
        "primary_keys": ["order_id", "payment_sequential"],
        "columns": ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]
    },
    "order_reviews": {
        "primary_keys": ["review_id"],
        "columns": ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message",
                   "review_creation_date", "review_answer_timestamp"]
    },
    "geolocation": {
        "primary_keys": ["geolocation_id"],
        "columns": ["geolocation_id", "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
                   "geolocation_city", "geolocation_state"]
    },
    "product_category_name_translation": {
        "primary_keys": ["product_category_name"],
        "columns": ["product_category_name", "product_category_name_english"]
    }
}

def create_spark_session() -> SparkSession:
    
    spark = SparkSession.builder \
        .appName('Kafka-to-Bronze-CDC-SQL') \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.bucket.create.enabled", "true") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "50") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URIS) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.metricsEnabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel('WARN')
    
    return spark

def create_or_update_catalog(spark: SparkSession, table_name: str, delta_path: str):
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        
        table_exists_in_catalog = spark.catalog.tableExists(f"bronze.{table_name}")
        
        if not table_exists_in_catalog:
            print(f"Creating HMS catalog entry for bronze.{table_name}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS bronze.{table_name}
                USING DELTA
                LOCATION '{delta_path}'
            """)
            print(f"Catalog entry created: bronze.{table_name}")
        else:
            spark.sql(f"REFRESH TABLE bronze.{table_name}")
            print(f"Catalog entry refreshed: bronze.{table_name}")
            
    except Exception as e:
        print(f"WARNING: Failed to create/update catalog for {table_name}: {e}")
        print(f"Data is still available at: {delta_path}")

def process_cdc_batch(batch_df, batch_id: int, table_name: str, delta_path: str):

    try:
        if batch_df.isEmpty():
            return
        
        spark = batch_df.sparkSession
        
        record_count = batch_df.count()
        print(f"[Batch {batch_id}] {table_name}: Processing {record_count} records")
        
        # Register batch as temp view
        batch_df.createOrReplaceTempView("cdc_batch")
        
        config = TABLE_CONFIG.get(table_name)
        if not config:
            print(f"ERROR: No config for table {table_name}")
            return
        
        columns = config["columns"]
        primary_keys = config["primary_keys"]
        
        # Build column list for SELECT and schema
        col_list = ", ".join(columns)
        struct_schema = ", ".join([f"{col}:STRING" for col in columns])
        
        # Build the CDC schema for from_json
        cdc_schema = f"""before STRUCT<{struct_schema}>, 
                after STRUCT<{struct_schema}>, 
                op STRING, 
                ts_ms BIGINT, 
                source STRUCT<version:STRING,
                connector:STRING,
                name:STRING,
                ts_ms:BIGINT,
                snapshot:STRING,
                db:STRING,
                schema:STRING,
                table:STRING,
                txId:BIGINT,
                lsn:BIGINT,
                xmin:BIGINT>"""
        
        # STEP 1: Parse CDC Message
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
            FROM cdc_batch
        )
        """
        parsed_df = spark.sql(parsed_sql)
        parsed_df.createOrReplaceTempView("parsed_cdc")
        print(f"[Batch {batch_id}] {table_name}: Parsed {parsed_df.count()} CDC messages")
        
        # STEP 2: Deduplicate 
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
        deduped_df.createOrReplaceTempView("source_data")
        final_count = deduped_df.count()
        print(f"[Batch {batch_id}] {table_name}: After dedup: {final_count} records")
        
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
                "processed_at = source.processed_at"
            ])
            set_clause = ", ".join(update_columns)
            
            all_columns = columns + ["operation_type", "cdc_operation", "cdc_timestamp_ms", "processed_at"]
            insert_cols = ", ".join(all_columns)
            insert_vals = ", ".join([f"source.{col}" for col in all_columns])
            
            # Execute MERGE: DELETE / UPDATE / INSERT
            merge_sql = f"""
            MERGE INTO delta.`{delta_path}` AS target
            USING source_data AS source
            ON {merge_condition}
            WHEN MATCHED AND source.is_deleted = true THEN
                DELETE
            WHEN MATCHED AND source.is_deleted = false THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals})
            """
            
            spark.sql(merge_sql)
            print(f"[Batch {batch_id}] {table_name}: MERGE completed")
            
        else:
            # Create new Delta table
            deduped_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(delta_path)
            
            print(f"[Batch {batch_id}] {table_name}: Created Delta table with {final_count} records")
            create_or_update_catalog(spark, table_name, delta_path)
    
    except Exception as e:
        print(f"[Batch {batch_id}] {table_name}: ERROR in batch processing: {e}")
        import traceback
        traceback.print_exc()

def start_streaming(spark: SparkSession, table_name: str):
    
    delta_path = f"{BRONZE_PATH}/{table_name}"
    checkpoint_path = f"{CHECKPOINT_PATH}/bronze/{table_name}"
    
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", table_name) \
        .option("startingOffsets", STARTING_OFFSETS) \
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER) \
        .option("failOnDataLoss", "false") \
        .load()
    
    query = kafka_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .foreachBatch(lambda batch_df, batch_id: process_cdc_batch(
            batch_df, batch_id, table_name, delta_path
        )) \
        .trigger(processingTime='30 seconds') \
        .start()
    
    return query

def main():
    tables = list(TABLE_CONFIG.keys())
    
    print(f"Tables: {', '.join(tables)}")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Bronze: {BRONZE_PATH}")
    print(f"Hive Metastore: {HIVE_METASTORE_URIS}")
    
    spark = create_spark_session()
    
    queries = []
    for table_name in tables:
        try:
            query = start_streaming(spark, table_name)
            queries.append(query)
            print(f"Stream started: {table_name}")
        except Exception as e:
            print(f"ERROR: Failed to start stream for {table_name}: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\nStarted {len(queries)}/{len(tables)} streaming queries")
    
    if queries:
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            print("\nStopping all streams...")
            for query in queries:
                query.stop()
            print("All streams stopped")

if __name__ == "__main__":
    main()

