import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from pathlib import Path

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'postgres'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

if os.path.exists('/app/data/ecommerce'):
    # Running in container
    DATA_DIR = Path('/app/data/ecommerce')
else:
    # Running locally
    DATA_DIR = Path(__file__).parent.parent / 'data' / 'ecommerce'

TABLES_CONFIG = [
    {
        'name': 'product_category_name_translation',
        'file': 'product_category_name_translation.csv',
        'chunksize': 100
    },
    {
        'name': 'customers',
        'file': 'olist_customers_dataset.csv',
        'chunksize': 10000
    },
    {
        'name': 'sellers',
        'file': 'olist_sellers_dataset.csv',
        'chunksize': 5000
    },
    {
        'name': 'products',
        'file': 'olist_products_dataset.csv',
        'chunksize': 10000
    },
    {
        'name': 'orders',
        'file': 'olist_orders_dataset.csv',
        'chunksize': 10000
    },
    {
        'name': 'order_items',
        'file': 'olist_order_items_dataset.csv',
        'chunksize': 10000
    },
    {
        'name': 'order_payments',
        'file': 'olist_order_payments_dataset.csv',
        'chunksize': 10000
    },
    {
        'name': 'order_reviews',
        'file': 'olist_order_reviews_dataset.csv',
        'chunksize': 10000
    },
    {
        'name': 'geolocation',
        'file': 'olist_geolocation_dataset.csv',
        'chunksize': 50000
    }
]

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"Connected to: {DB_CONFIG['database']}")
        return conn
    except Exception as e:
        print(f"Failed to connect: {e}")
        raise

def clean_dataframe(df, table_name):
    df = df.where(pd.notnull(df), None)
    return df

def load_table(conn, table_config):
    table_name = table_config['name']
    csv_file = DATA_DIR / table_config['file']
    chunksize = table_config['chunksize']
    
    if not csv_file.exists():
        print(f"CSV file not found: {csv_file}")
        return

    print(f"Loading data into table: {table_name}")
    
    try:
        total_rows = 0
        
        for chunk_num, chunk_df in enumerate(pd.read_csv(csv_file, chunksize=chunksize), 1):
            chunk_df = clean_dataframe(chunk_df, table_name)
            
            columns = chunk_df.columns.tolist()
            values = [tuple(row) for row in chunk_df.values]
            
            insert_query = sql.SQL(
                "INSERT INTO {table} ({fields}) VALUES ({placeholders})"
            ).format(
                table=sql.Identifier(table_name),
                fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
                placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns))
            )
            
            with conn.cursor() as cur:
                execute_batch(cur, insert_query, values, page_size=1000)
                conn.commit()
            
            total_rows += len(chunk_df)
            print(f"  Chunk {chunk_num}: Inserted {len(chunk_df)} rows (Total: {total_rows})")

        print(f"Successfully loaded {total_rows} rows into {table_name}")

    except Exception as e:
        print(f"Failed to load data into {table_name}: {e}")
        conn.rollback()
        raise

def verify_data(conn):
    print("\n" + "="*60)
    print("DATA VERIFICATION")

    with conn.cursor() as cur:
        for table_config in TABLES_CONFIG:
            table_name = table_config['name']
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]
            print(f"{table_name:40s}: {count:>10,} rows")

def main():

    conn = None
    try:
        conn = get_db_connection()

        print("Loading data from CSV files")

        for table_config in TABLES_CONFIG:
            load_table(conn, table_config)

        verify_data(conn)

        print("DATA LOADING COMPLETED SUCCESSFULLY")

    except Exception as e:
        print(f"\nDATA LOADING FAILED: {e}")
        raise
    finally:
        if conn:
            conn.close()
            print("Database connection closed")

if __name__ == '__main__':
    main()

