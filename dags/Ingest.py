from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from sqlalchemy.engine import URL, create_engine
from sqlalchemy.types import Integer, Float, String, Boolean, TIMESTAMP, Text
import fastparquet
import pandas as pd
import fastavro
import psycopg2
import json
from pprint import pprint

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 12),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

def postgres_connection():
    connection_string = URL.create(
        'postgresql',
        username='Data_Warehouse_owner',
        password='1evSBNImwxV8',
        host='ep-still-grass-a5bz365g.us-east-2.aws.neon.tech',
        database='Data_Warehouse',
        port=5432,
        query={'sslmode': 'require'}
    )
    
    engine = create_engine(connection_string)
    return engine

def insert_to_postgres():
    engine = postgres_connection()
    path = 'data/order_item.avro'
    with open(path, 'rb') as f:
        avro_reader = fastavro.reader(f)
        data = list(avro_reader)

    df = pd.DataFrame(data)
    df_schema = {
        'id': Integer,
        'order_id': Integer,
        'product_id': Integer,
        'amount': Integer,
        'coupon_id': Integer
    }

    df.to_sql('order_items', con=engine, schema='public', if_exists='replace', index=False, dtype=df_schema)

def insert_coupon_to_postgres():
    engine = postgres_connection()
    with open('data/coupons.json') as file_open:
        json_open = json.load(file_open)

    df = pd.DataFrame(json_open)

    df_schema = {
        'id': Integer,
        'discount_percent': Float
    }

    df.to_sql('coupons', con=engine, index=False, if_exists='replace', schema='public', dtype=df_schema)

def insert_customer_csv_to_postgres():
    engine = postgres_connection()
    schema = {
        'id': Integer,
        'first_name': String,
        'last_name': String,
        'gender': String,
        'address': String,
        'zip_code': String
    }
    data = pd.DataFrame(columns=['id', 'first_name', 'last_name', 'gender', 'address', 'zip_code'])
    for i in range(10):
        print(f'Mengekstrak csv {i}')
        data_csv = pd.read_csv(f'data/customer_{i}.csv')
        data_csv.drop('Unnamed: 0', axis=1, inplace=True)
        data = pd.concat([data, data_csv])
        print(f'Sudah berhasil ekstrak data')
    data.to_sql('customers', con=engine, schema='public', if_exists='replace', index=False, dtype=schema)
    print(f'Sudah berhasil eksekusi sql {len(data)}')

def insert_login_attempts():
    engine = postgres_connection()
    nomer = 0
    all_data = []
    for i in range(10):
        data = []
        with open(f'data/login_attempts_{i}.json') as file_open:
            print(f'Membuka file {i}')
            json_open = json.load(file_open)
            jumlah = len(json_open['id'])
            for j in range(jumlah):
                index = f'{nomer}'
                id = json_open['id'][index]
                customer_id = json_open['customer_id'][index]
                login_successful = json_open['login_successful'][index]
                timestamp = json_open['attempted_at'][index]
                attempted_at = datetime.fromtimestamp(timestamp / 1000)
                tupple = (id, customer_id, login_successful, attempted_at)
                data.append(tupple)
                nomer += 1
            print("Sudah berhasil ekstrak data")
            df = pd.DataFrame(data, columns=['id', 'customer_id', 'login_succesful', 'attempted_at'])
            all_data.append(df)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.to_sql('login_attempt_history', con=engine, index=False, if_exists='replace', schema='public', dtype={
            'id': Integer,
            'customer_id': Integer,
            'login_succesfull': Boolean,
            'attempted_at': TIMESTAMP
        })

def insert_product_category():
    engine = postgres_connection()
    sheet = pd.read_excel('data/product_category.xls')
    sheet.convert_dtypes()
    sheet.to_sql('product_categories', con=engine, index=False, if_exists='replace', schema='public', dtype={
        'id': Integer,
        'name': String
    })

def insert_products():
    engine = postgres_connection()
    source = pd.read_excel('data/product.xls')
    source.convert_dtypes()
    source.to_sql('products', con=engine, index=False, if_exists='replace', schema='public', dtype={
        'id': Integer,
        'name': String,
        'price': Float,
        'category_id': Integer,
        'supplier_id': Integer
    })

def insert_supplier_data():
    engine = postgres_connection()
    source = pd.read_excel('data/supplier.xls')
    source.convert_dtypes()
    source.to_sql('suppliers', con=engine, index=False, if_exists='replace', schema='public', dtype={
        'id': Integer,
        'name': String,
        'country': String
    })

def insert_order_data():
    engine = postgres_connection()
    file = 'data/order.parquet'
    df = pd.read_parquet(file)
    schema = {
        'id': Integer,
        'customer_id': Integer,
        'status': Text,
        'created_at': TIMESTAMP
    }
    df.to_sql('orders', con=engine, schema='public', if_exists='replace', index=False, dtype=schema)

with DAG(
    dag_id='main_dag',
    default_args=default_args,
    schedule_interval='@once',
) as dag:

    task_ingest_avro_to_postgres = PythonOperator(
        task_id='avro_to_postgres',
        python_callable=insert_to_postgres,
        dag=dag
    )

    task_ingest_coupons_json_to_postgres = PythonOperator(
        task_id='coupons_to_postgres',
        python_callable=insert_coupon_to_postgres,
        provide_context=True,
        dag=dag
    )

    task_ingest_customer_to_postgres = PythonOperator(
        task_id='customer_to_postgres',
        python_callable=insert_customer_csv_to_postgres,
        dag=dag
    )

    task_ingest_login_attempts_to_postgres = PythonOperator(
        task_id='login_attempts_to_postgres',
        python_callable=insert_login_attempts,
        dag=dag
    )

    task_ingest_product_category_to_postgres = PythonOperator(
        task_id='product_category_to_postgres',
        python_callable=insert_product_category,
        dag=dag
    )

    task_ingest_products_to_postgres = PythonOperator(
        task_id='products_to_postgres',
        python_callable=insert_products,
        dag=dag
    )

    task_ingest_suppliers_to_postgres = PythonOperator(
        task_id='suppliers_to_postgres',
        python_callable=insert_supplier_data,
        dag=dag
    )

    task_ingest_orders_to_postgres = PythonOperator(
        task_id='orders_to_postgres',
        python_callable=insert_order_data,
        dag=dag
    )

    task_ingest_avro_to_postgres >> task_ingest_coupons_json_to_postgres >> task_ingest_customer_to_postgres >> task_ingest_login_attempts_to_postgres >> task_ingest_product_category_to_postgres >> task_ingest_products_to_postgres >> task_ingest_suppliers_to_postgres >> task_ingest_orders_to_postgres