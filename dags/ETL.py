#Airflow
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.edgemodifier import Label
from airflow.operators.postgres_operator import PostgresOperator
from datetime import timedelta, datetime
import pandas as pd
import polars as pl
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Float, DateTime

default = {
    "owner" : "Kelompok 16",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=5),
}

def postgres_connecion():
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

KEY_DATA_COUPONS = "df_coupons"
KEY_DATA_CUSTOMERS = "df_customers"
KEY_DATA_LOGIN= "df_login"
KEY_DATA_ORDER_ITEMS="df_order_items"
KEY_DATA_ORDER="df_orders"
KEY_DATA_PRODUCT_CATEGORIES= "df_product_categories"
KEY_DATA_PRODUCTS="df_data_products"
KEY_DATA_SUPPLIERS="df_suppliers"

with DAG(
    dag_id="ETL_Kelompok_16",
    start_date=datetime(2023, 11, 21),
    catchup=False,
    default_args=default,
    schedule_interval="@once"
) as dag :

    @task
    def fetch_data_json_coupons(**context):
        file_coupon = [
          'data/coupons.json',
        ]

        df_result = pd.DataFrame()
        for path in file_coupon:
            df_temp = pd.read_json(path)
            # df_result = df_result.append(df_temp) #pakai pandas >= 1.4.0 akan error
            df_result = pd.concat([df_result,df_temp])
        print(df_result)
        return context['ti'].xcom_push(key=KEY_DATA_COUPONS, value=df_result)

    @task
    def fetch_data_from_csv_customer(**context):
        file_customer = [
            'data/customer_0.csv', #file path disesuaikan
            'data/customer_1.csv',
            'data/customer_2.csv',
            'data/customer_3.csv',
            'data/customer_4.csv',
            'data/customer_5.csv',
            'data/customer_6.csv',
            'data/customer_7.csv',
            'data/customer_8.csv',
            'data/customer_9.csv'
        ]

        df_result = pd.DataFrame()
        for path in file_customer:
            df_temp = pd.read_csv(path)
            # df_result = df_result.append(df_temp) #pakai pandas >= 1.4.0 akan error
            df_result = pd.concat([df_result,df_temp])
            print("test_result", df_result)
        return context['ti'].xcom_push(key=KEY_DATA_CUSTOMERS, value=df_result)

    @task
    def fetch_data_from_json_login(**context):

        file_login_attempts = [
            'data/login_attempts_0.json', #file path disesuaikan
            'data/login_attempts_1.json',
            'data/login_attempts_2.json',
            'data/login_attempts_3.json',
            'data/login_attempts_4.json',
            'data/login_attempts_5.json',
            'data/login_attempts_6.json',
            'data/login_attempts_7.json',
            'data/login_attempts_8.json',
            'data/login_attempts_9.json'
        ]

        df_result = pd.DataFrame()
        for path in file_login_attempts:
            df_temp = pd.read_json(path)
            # df_result = df_result.append(df_temp) #pakai pandas >= 1.4.0 akan error
            df_result = pd.concat([df_result,df_temp])
        return context['ti'].xcom_push(key=KEY_DATA_LOGIN, value=df_result)


    @task
    def fetch_data_from_avro_order_item(**context):
        file_avro = [
            'data/order_item.avro'
        ]
        pl_result = pl.DataFrame() #menggunakan polars dataframe
        for path in file_avro:
            pl_temp = pl.read_avro(path)
            pl_result = pl.concat([pl_temp], how="vertical")
        df_orders_items = pl_result.to_pandas()
        return context['ti'].xcom_push(key=KEY_DATA_ORDER_ITEMS, value=df_orders_items)


    @task
    def fetch_data_from_excel_product_categories(**context):

        file_product_categories = [
            'data/product_category.xls'
        ]

        df_result = pd.DataFrame()
        for path in file_product_categories:
            df_temp = pd.read_excel(path)
            # df_result = df_result.append(df_temp) #pakai pandas >= 1.4.0 akan error
            df_result = pd.concat([df_result,df_temp])

        return context['ti'].xcom_push(key=KEY_DATA_PRODUCT_CATEGORIES, value=df_result)


    @task
    def fetch_data_from_excel_products(**context):

        file_products = [
            'data/product.xls'
        ]

        df_result = pd.DataFrame()
        for path in file_products:
            df_temp = pd.read_excel(path)
            # df_result = df_result.append(df_temp) #pakai pandas >= 1.4.0 akan error
            df_result = pd.concat([df_result,df_temp])

        return context['ti'].xcom_push(key=KEY_DATA_PRODUCTS, value=df_result)

    @task
    def fetch_data_from_excel_suppliers(**context):

        file_suppliers = [
            'data/supplier.xls'
        ]

        df_result = pd.DataFrame()
        for path in file_suppliers:
            df_temp = pd.read_excel(path)
            # df_result = df_result.append(df_temp) #pakai pandas >= 1.4.0 akan error
            df_result = pd.concat([df_result,df_temp])

        return context['ti'].xcom_push(key=KEY_DATA_SUPPLIERS, value=df_result)


    @task
    def fetch_data_from_parquet_order(**context):

        file_order = [
            'data/order.parquet'
        ]

        df_result = pd.DataFrame()
        for path in file_order:
            df_temp = pq.read_table(path)
            df_temp = df_temp.to_pandas()
            # df_result = df_result.append(df_temp) #pakai pandas >= 1.4.0 akan error
            df_result = pd.concat([df_result,df_temp])
        return context['ti'].xcom_push(key=KEY_DATA_ORDER, value=df_result)


    @task
    def transfrom_dataset(**context):
        #Transform Coupons Data
        df_coupon = context['ti'].xcom_pull(key=KEY_DATA_COUPONS)
        df_coupon['discount_percent'] = df_coupon['discount_percent'].astype(float)
        df_coupon.drop_duplicates(keep='last', inplace=True, subset=['id'])

        #Transform Customers Data
        df_customer = context['ti'].xcom_pull(key=KEY_DATA_CUSTOMERS)
        df_customer = df_customer.drop(columns=["Unnamed: 0"])
        df_customer['zip_code'] = df_customer['zip_code'].astype(str)
        df_customer.drop_duplicates(keep='last', inplace=True, subset=['id'])

        #Transform Login Attemps Data
        df_login_attemps = context['ti'].xcom_pull(key=KEY_DATA_LOGIN)
        df_login_attemps.drop_duplicates(keep='last', inplace=True, subset=['id'])

        #Transform Order Item Data
        df_order_items = context['ti'].xcom_pull(key=KEY_DATA_ORDER_ITEMS)
        df_order_items.drop_duplicates(keep='last', inplace=True, subset=['id'])

        #Transform Orders Data
        df_order = context['ti'].xcom_pull(key=KEY_DATA_ORDER)
        df_order['created_at'] = df_order['created_at'].astype('datetime64[ns]')

        #Transform Product Categories Data
        df_product_categories = context['ti'].xcom_pull(key=KEY_DATA_PRODUCT_CATEGORIES)
        df_product_categories = df_product_categories.drop(columns=["Unnamed: 0"])
        df_product_categories.drop_duplicates(keep='last', inplace=True, subset=['id'])

        #Transform Products
        df_product = context['ti'].xcom_pull(key=KEY_DATA_PRODUCTS)
        df_product = df_product.drop(columns=["Unnamed: 0"])
        df_product['name'] = df_product['name'].astype(str)
        df_product['price'] = df_product['price'].astype(float)
        df_product.drop_duplicates(keep='last', inplace=True, subset=['id'])

        #Transorm Suppliers
        df_suppliers = context['ti'].xcom_pull(key=KEY_DATA_SUPPLIERS)
        df_suppliers = df_suppliers.drop(columns=["Unnamed: 0"])
        df_suppliers.drop_duplicates(keep='last', inplace=True, subset=['id'])

        context['ti'].xcom_push(key=KEY_DATA_COUPONS, value=df_coupon)
        context['ti'].xcom_push(key=KEY_DATA_CUSTOMERS, value=df_customer)
        context['ti'].xcom_push(key=KEY_DATA_LOGIN, value=df_login_attemps)
        context['ti'].xcom_push(key=KEY_DATA_ORDER, value=df_order)
        context['ti'].xcom_push(key=KEY_DATA_ORDER_ITEMS, value=df_order_items)
        context['ti'].xcom_push(key=KEY_DATA_PRODUCT_CATEGORIES, value=df_product_categories)
        context['ti'].xcom_push(key=KEY_DATA_PRODUCTS, value=df_product)
        context['ti'].xcom_push(key=KEY_DATA_SUPPLIERS, value=df_suppliers)


    @task
    def insert_to_database(**context):
        df_coupons = context['ti'].xcom_pull(key=KEY_DATA_COUPONS)
        df_customers = context['ti'].xcom_pull(key=KEY_DATA_CUSTOMERS)
        df_login_attemps = context['ti'].xcom_pull(key=KEY_DATA_LOGIN)
        df_order_items = context['ti'].xcom_pull(key=KEY_DATA_ORDER_ITEMS)
        df_orders = context['ti'].xcom_pull(key=KEY_DATA_ORDER)
        df_product_categories = context['ti'].xcom_pull(key=KEY_DATA_PRODUCT_CATEGORIES)
        df_product = context['ti'].xcom_pull(key=KEY_DATA_PRODUCTS)
        df_suppliers = context['ti'].xcom_pull(key=KEY_DATA_SUPPLIERS)

        hook = PostgresHook(postgres_conn_id='postgres_dw')
        engine = hook.get_sqlalchemy_engine()

        # Note = You have to create schema Data Modelling Metabase on your database or you can change it to public        
        print("=== SEDANG INSERT COUPON")
        df_coupons.to_sql(name='coupons',schema  ='public', con=engine,
                            if_exists='replace', index=False)

        print("=== SEDANG INSERT CUSTOMERS")
        df_customers.to_sql(name='customer',schema = 'public', con=engine,
                            if_exists='replace', index=False)

        print("=== SEDANG INSERT LOGIN ATTEMPS")
        df_login_attemps.to_sql(name='login_attemps',schema='public',con=engine,
                           if_exists='replace', index=False)

        print("=== SEDANG INSERT ORDERS ITEMS")
        df_order_items.to_sql(name='order_items', schema='public', con=engine,
                           if_exists='replace', index=False)

        print("=== SEDANG INSERT ORDERS")
        df_orders.to_sql(name='orders',schema='public', con=engine,
                           if_exists='replace', index=False)

        print("=== SEDANG INSERT PRODUCT CATEGORIES")
        df_product_categories.to_sql(name='product_categories',schema='public',con=engine,
                           if_exists='replace', index=False)

        print("=== SEDANG INSERT PRODUCT")
        df_product.to_sql(name='products',schema='public',con=engine,
                           if_exists='replace', index=False)

        print("=== SEDANG INSERT SUPPLIERS")
        df_suppliers.to_sql(name='suppliers',schema='public',con=engine,
                           if_exists='replace', index=False)

    [fetch_data_json_coupons(),
     fetch_data_from_csv_customer(),
     fetch_data_from_json_login(),
     fetch_data_from_avro_order_item(),
     fetch_data_from_parquet_order(),
     fetch_data_from_excel_product_categories(),
     fetch_data_from_excel_products(),
     fetch_data_from_excel_suppliers()] >> transfrom_dataset() >> insert_to_database() 