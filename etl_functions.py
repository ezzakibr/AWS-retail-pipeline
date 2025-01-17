import boto3
import psycopg2
import time
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from config import *

def get_redshift_connection():
    """Create and return a Redshift connection"""
    return psycopg2.connect(
        dbname=REDSHIFT_DB,
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )

def check_s3_files(**context):
    """Check if required files exist in S3"""
    s3_hook = S3Hook()
    required_files = ['customers.csv', 'products.csv', 'orders.csv']
    
    for file in required_files:
        key = f'raw/{file}'
        if not s3_hook.check_for_key(key, bucket_name=S3_BUCKET):
            raise Exception(f"Required file {file} not found in raw folder")
    print("All required files found in S3")

def run_glue_crawler(**context):
    """Run the existing Glue crawler"""
    try:
        glue_client = boto3.client('glue', region_name=AWS_REGION)
        
        # Start the crawler
        print("Starting crawler...")
        glue_client.start_crawler(Name='retail_data_crawler')
        
        # Wait for completion
        while True:
            response = glue_client.get_crawler(Name='retail_data_crawler')
            status = response['Crawler']['State']
            
            if status == 'READY':
                print("Crawler completed successfully")
                break
            elif status == 'FAILED':
                raise Exception("Crawler failed")
                
            print(f"Crawler is {status}. Waiting...")
            time.sleep(30)
            
    except Exception as e:
        raise Exception(f"Error running Glue crawler: {str(e)}")

def load_to_staging(**context):
    """Load data from S3 to Redshift staging tables"""
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()

        copy_commands = {
            'stg_customers': f"""
                COPY stg_customers
                FROM 's3://{S3_BUCKET}/raw/customers.csv'
                IAM_ROLE '{REDSHIFT_ROLE}'
                FORMAT CSV
                IGNOREHEADER 1;
            """,
            'stg_products': f"""
                COPY stg_products
                FROM 's3://{S3_BUCKET}/raw/products.csv'
                IAM_ROLE '{REDSHIFT_ROLE}'
                FORMAT CSV
                IGNOREHEADER 1;
            """,
            'stg_orders': f"""
                COPY stg_orders
                FROM 's3://{S3_BUCKET}/raw/orders.csv'
                IAM_ROLE '{REDSHIFT_ROLE}'
                FORMAT CSV
                IGNOREHEADER 1;
            """
        }

        for table, copy_command in copy_commands.items():
            print(f"Loading {table}...")
            cur.execute(copy_command)

        conn.commit()
        print("All staging tables loaded successfully")

    except Exception as e:
        raise Exception(f"Error loading staging tables: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

def transform_to_dim_fact(**context):
    """Transform staging data into dimension and fact tables"""
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()

        # Populate dim_customers
        print("Populating dim_customers...")
        cur.execute("""
        INSERT INTO dim_customers (customer_id, first_name, last_name, email, registration_date)
        SELECT 
            customer_id, 
            first_name, 
            last_name, 
            email, 
            CAST(registration_date AS TIMESTAMP)
        FROM stg_customers;
        """)

        # Populate dim_products
        print("Populating dim_products...")
        cur.execute("""
        INSERT INTO dim_products (product_id, product_name, category, price, stock_quantity)
        SELECT product_id, product_name, category, price, stock_quantity
        FROM stg_products;
        """)

        # Populate dim_date
        print("Populating dim_date...")
        cur.execute("""
        INSERT INTO dim_date (date_key, full_date, year, month, day, quarter, is_weekend)
        SELECT DISTINCT
            CAST(TO_CHAR(CAST(order_date AS TIMESTAMP), 'YYYYMMDD') AS INTEGER) as date_key,
            CAST(order_date AS DATE) as full_date,
            EXTRACT(year FROM CAST(order_date AS TIMESTAMP)) as year,
            EXTRACT(month FROM CAST(order_date AS TIMESTAMP)) as month,
            EXTRACT(day FROM CAST(order_date AS TIMESTAMP)) as day,
            EXTRACT(quarter FROM CAST(order_date AS TIMESTAMP)) as quarter,
            CASE WHEN EXTRACT(DOW FROM CAST(order_date AS TIMESTAMP)) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend
        FROM stg_orders;
        """)

        # Populate fact_orders
        print("Populating fact_orders...")
        cur.execute("""
        INSERT INTO fact_orders (
            order_id, customer_key, product_key, date_key,
            quantity, unit_price, total_amount
        )
        SELECT 
            o.order_id,
            c.customer_key,
            p.product_key,
            CAST(TO_CHAR(CAST(o.order_date AS TIMESTAMP), 'YYYYMMDD') AS INTEGER) as date_key,
            o.quantity,
            o.unit_price,
            o.total_amount
        FROM stg_orders o
        JOIN dim_customers c ON o.customer_id = c.customer_id
        JOIN dim_products p ON o.product_id = p.product_id;
        """)

        conn.commit()
        print("All dimension and fact tables populated successfully")

    except Exception as e:
        raise Exception(f"Error transforming data: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

def unload_to_processed(**context):
    """Unload transformed data to S3 processed zone"""
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()

        tables = {
            'dim_customers': 'SELECT * FROM dim_customers ORDER BY customer_key',
            'dim_products': 'SELECT * FROM dim_products ORDER BY product_key',
            'dim_date': 'SELECT * FROM dim_date ORDER BY date_key',
            'fact_orders': 'SELECT * FROM fact_orders ORDER BY order_id'
        }

        for table_name, query in tables.items():
            print(f"Unloading {table_name}...")
            unload_query = f"""
            UNLOAD ('{query}')
            TO 's3://{S3_BUCKET}/processed/{table_name}/data'
            IAM_ROLE '{REDSHIFT_ROLE}'
            CSV
            HEADER
            PARALLEL OFF
            ALLOWOVERWRITE
            """
            cur.execute(unload_query)

        print("All tables unloaded to processed zone successfully")

    except Exception as e:
        raise Exception(f"Error unloading data: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()