from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.engine import URL, create_engine
from datetime import datetime, timedelta
import pandas as pd
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 12),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def source_postgres_connection():
    connection_string = URL.create(
        'postgresql',
        username='data_warehouse_owner',
        password='cuwzUmI4el8g',
        host='ep-curly-dust-a5e55r4w.us-east-2.aws.neon.tech',
        database='data_warehouse',
        port=5432,
        query={'sslmode': 'require'}
    )
    engine = create_engine(connection_string)
    return engine

def target_postgres_connection():
    connection_string = URL.create(
        drivername='postgresql',
        username='data_warehouse2_owner',
        password='6HiSMm3tTOoz',
        host='ep-dry-lake-a5itpxp1.us-east-2.aws.neon.tech',
        port=5432,
        database='data_warehouse2',
        query={'sslmode': 'require'}
    )
    engine = create_engine(connection_string)
    return engine

def create_dim_coupons_table():
    logging.info("Creating dim_coupons table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_coupons (
                    coupon_id SERIAL PRIMARY KEY,
                    discount_percent TEXT
                );
            """)
        logging.info("Table 'dim_coupons' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_coupons': {e}")
        raise

def transfer_coupons_data():
    logging.info("Transferring data from coupons to dim_coupons.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = "SELECT id AS coupon_id, discount_percent FROM coupons"
            coupons_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            coupons_df.to_sql('dim_coupons', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'coupons' to 'dim_coupons'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

def create_dim_customers_table():
    logging.info("Creating dim_customers table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_customers (
                    customer_id SERIAL PRIMARY KEY,
                    first_name VARCHAR,
                    last_name VARCHAR,
                    gender VARCHAR,
                    address VARCHAR,
                    zip_code VARCHAR
                );
            """)
        logging.info("Table 'dim_customers' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_customers': {e}")
        raise

def transfer_customers_data():
    logging.info("Transferring data from customer to dim_customers.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = "SELECT id AS customer_id, first_name, last_name, gender, address, zip_code FROM customer"
            customers_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            customers_df.to_sql('dim_customers', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'customer' to 'dim_customers'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

def create_dim_login_attempt_history_table():
    logging.info("Creating dim_login_attempt_history table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_login_attempt_history (
                    attempt_id SERIAL PRIMARY KEY,
                    customer_id INT,
                    attempted_at date,
                    login_successful boolean
                );
            """)
        logging.info("Table 'dim_login_attempt_history' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_login_attempt_history': {e}")
        raise

def transfer_login_attempt_history_data():
    logging.info("Transferring data from  login_attemps to dim_login_attempt_history.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = " SELECT id AS attempt_id, customer_id, attempted_at, login_successful FROM  login_attemps"
            login_attempt_history_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            login_attempt_history_df.to_sql('dim_login_attempt_history', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'login_attemps' to 'dim_login_attempt_history'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

def create_dim_product_categories_table():
    logging.info("Creating dim_product_categories table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_categories (
                    category_id SERIAL PRIMARY KEY,
                    name VARCHAR
                );
            """)
        logging.info("Table 'dim_product_categories' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_product_categories': {e}")
        raise

def transfer_product_categories_data():
    logging.info("Transferring data from product_categories to dim_product_categories.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = " SELECT id AS category_id, name FROM product_categories"
            product_categories_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            product_categories_df.to_sql('dim_product_categories', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'product_categories' to 'dim_product_categories'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

def create_dim_products_table():
    logging.info("Creating dim_products table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_products (
                    product_id SERIAL PRIMARY KEY,
                    name VARCHAR,
                    price FLOAT,
                    category_id INT,
                    supplier_id INT
                );
            """)
        logging.info("Table 'dim_products' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_products': {e}")
        raise

def transfer_products_data():
    logging.info("Transferring data from products to dim_products.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = " SELECT id AS product_id, name, price, category_id, supplier_id FROM products"
            products_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            products_df.to_sql('dim_products', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'products' to 'dim_products'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

def create_dim_orders_table():
    logging.info("Creating dim_orders table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_orders (
                    orders_id SERIAL PRIMARY KEY,
                    customer_id integer,
                    status text,
                    created_at timestamp
                );
            """)
        logging.info("Table 'dim_orders' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_orders': {e}")
        raise

def transfer_orders_data():
    logging.info("Transferring data from orders to dim_orders.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = " SELECT id AS orders_id, customer_id, status, created_at FROM orders"
            orders_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            orders_df.to_sql('dim_orders', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'orders' to 'dim_orders'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

def create_dim_suppliers_table():
    logging.info("Creating dim_suppliers table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_suppliers (
                    supplier_id SERIAL PRIMARY KEY,
                    name VARCHAR,
                    country VARCHAR
                );
            """)
        logging.info("Table 'dim_suppliers' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_suppliers': {e}")
        raise

def transfer_suppliers_data():
    logging.info("Transferring data from suppliers to dim_suppliers.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = "SELECT id AS supplier_id, name, country FROM suppliers"
            suppliers_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            suppliers_df.to_sql('dim_suppliers', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'suppliers' to 'dim_suppliers'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

def create_dim_order_items_table():
    logging.info("Creating dim_order_items table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_order_items (
                    order_items_id SERIAL PRIMARY KEY,
                    order_id INT,
                    product_id INT,
                    amount INT,
                    coupon_id INT
                );
            """)
        logging.info("Table 'dim_order_items' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_order_items': {e}")
        raise

def transfer_order_items_data():
    logging.info("Transferring data from order_items to dim_order_items.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = "SELECT id AS order_items_id, order_id, product_id, amount, coupon_id FROM order_items"
            order_items_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            order_items_df.to_sql('dim_order_items', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'order_items' to 'dim_order_items'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

def create_fact_order_details_table():
    logging.info("Creating fact_order_details table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS fact_order_details (
                    order_id SERIAL PRIMARY KEY,
                    customer_id INT,
                    status TEXT,
                    created_at DATE,
                    product_id INT,
                    amount INT,
                    coupon_id INT
                );
            """)
        logging.info("Table 'fact_order_details' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'fact_order_details': {e}")
        raise

def transfer_order_details_data():
    logging.info("Transferring data from transf_order_details to fact_order_details.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = """
                WITH orders AS (
                    SELECT
                        oi.id AS order_id,
                        o.customer_id,
                        o.status,
                        o.created_at,
                        oi.product_id,
                        oi.amount,
                        oi.coupon_id
                    FROM orders o
                    JOIN order_items oi ON o.id = oi.order_id
                )
                SELECT * FROM orders
            """
            order_details_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            order_details_df.to_sql('fact_order_details', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'transf_order_details' to 'fact_order_details'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

dag = DAG(
    'Modeling',
    default_args=default_args,
    description='DAG untuk membuat tabel baru dan mentransfer data',
    schedule_interval='@once',
)

create_dim_coupons = PythonOperator(
    task_id='create_dim_coupons',
    python_callable=create_dim_coupons_table,
    dag=dag,
)

transfer_coupons = PythonOperator(
    task_id='transfer_coupons',
    python_callable=transfer_coupons_data,
    dag=dag,
)

create_dim_customers = PythonOperator(
    task_id='create_dim_customers',
    python_callable=create_dim_customers_table,
    dag=dag,
)

transfer_customers = PythonOperator(
    task_id='transfer_customers',
    python_callable=transfer_customers_data,
    dag=dag,
)

create_dim_login_attempt_history = PythonOperator(
    task_id='create_dim_login_attempt_history',
    python_callable=create_dim_login_attempt_history_table,
    dag=dag,
)

transfer_login_attempt_history = PythonOperator(
    task_id='transfer_login_attempt_history',
    python_callable=transfer_login_attempt_history_data,
    dag=dag,
)

create_dim_product_categories = PythonOperator(
    task_id='create_dim_product_categories',
    python_callable=create_dim_product_categories_table,
    dag=dag,
)

transfer_product_categories = PythonOperator(
    task_id='transfer_product_categories',
    python_callable=transfer_product_categories_data,
    dag=dag,
)

create_dim_products = PythonOperator(
    task_id='create_dim_products',
    python_callable=create_dim_products_table,
    dag=dag,
)

transfer_products = PythonOperator(
    task_id='transfer_products',
    python_callable=transfer_products_data,
    dag=dag,
)

create_dim_orders = PythonOperator(
    task_id='create_dim_orders',
    python_callable=create_dim_orders_table,
    dag=dag,
)

transfer_orders = PythonOperator(
    task_id='transfer_orders',
    python_callable=transfer_orders_data,
    dag=dag,
)

create_dim_suppliers = PythonOperator(
    task_id='create_dim_suppliers',
    python_callable=create_dim_suppliers_table,
    dag=dag,
)

transfer_suppliers = PythonOperator(
    task_id='transfer_suppliers',
    python_callable=transfer_suppliers_data,
    dag=dag,
)

create_dim_order_items = PythonOperator(
    task_id='create_dim_order_items',
    python_callable=create_dim_order_items_table,
    dag=dag,
)

transfer_order_items = PythonOperator(
    task_id='transfer_order_items',
    python_callable=transfer_order_items_data,
    dag=dag,
)

create_fact_order_details = PythonOperator(
    task_id='create_fact_order_details',
    python_callable=create_fact_order_details_table,
    dag=dag,
)

transfer_order_details = PythonOperator(
    task_id='transfer_order_details',
    python_callable=transfer_order_details_data,
    dag=dag,
)

create_dim_coupons >> transfer_coupons
create_dim_customers >> transfer_customers
create_dim_login_attempt_history >> transfer_login_attempt_history
create_dim_product_categories >> transfer_product_categories
create_dim_products >> transfer_products
create_dim_orders >> transfer_orders
create_dim_suppliers >> transfer_suppliers
create_dim_order_items >> transfer_order_items
create_fact_order_details >> transfer_order_details