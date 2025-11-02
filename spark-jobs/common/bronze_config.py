from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'admin'
S3_SECRET_KEY = 'password123'
BRONZE_PATH = 's3a://bronze'
HIVE_METASTORE_URIS = 'thrift://hive-metastore:9083'

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

def create_spark_session(app_name: str = 'Bronze-CDC-Processing', streaming: bool = False) -> SparkSession:
    """
    Create Spark session with common configurations
    
    Args:
        app_name: Application name
        streaming: If True, add streaming-specific configs
    """
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "50") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URIS)
    
    if streaming:
        builder = builder \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.metricsEnabled", "false") \
            .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
            .config("spark.sql.streaming.stateStore.providerClass", 
                   "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
            .config("spark.default.parallelism", "2") \
            .config("spark.sql.shuffle.partitions", "2")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    
    return spark

def create_or_update_catalog(spark: SparkSession, table_name: str, delta_path: str):

    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        
        if not spark.catalog.tableExists(f"bronze.{table_name}"):
            spark.sql(f"""
                CREATE TABLE bronze.{table_name}
                USING DELTA
                LOCATION '{delta_path}'
            """)
        else:
            spark.sql(f"REFRESH TABLE bronze.{table_name}")
            
    except Exception as e:
        print(f"Catalog error for {table_name}: {e}")

def validate_table_config(table_name: str) -> dict:
    config = TABLE_CONFIG.get(table_name)
    if not config:
        raise ValueError(f"Table '{table_name}' not found in TABLE_CONFIG")
    return config
