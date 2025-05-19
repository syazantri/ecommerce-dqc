from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os

def parse_value(val):
    try:
        if pd.isna(val):
            return None
        if isinstance(val, str):
            val = val.replace('Rp', '').replace('%', '').replace('-', '').replace(',', '').replace('.', '').strip().upper()
            if val == '' or val in ['NA', 'N/A']:
                return None
            if 'K' in val:
                return int(float(val.replace('K', '')) * 1000)
            elif 'M' in val:
                return int(float(val.replace('M', '')) * 1_000_000)
            return int(float(val))
        elif isinstance(val, (int, float)):
            return int(val)
    except Exception as e:
        print(f"[parse_value] Failed parsing value: {val}, Error: {e}")
        return None

def load_csv_to_postgres():
    df = pd.read_csv('/opt/airflow/dags/data/raw_data.csv')

    df['ScrapDate'] = pd.to_datetime(df['ScrapDate'], errors='coerce').dt.date

    for col in ['SalesCount', 'SalePrice', 'OriginalPrice', 'Discount']:
        df[col] = df[col].apply(parse_value)

    print("Data types after cleaning:")
    print(df.dtypes)
    df = df.dropna(subset=['SalePrice', 'OriginalPrice'])

    df = df[(df['SalePrice'] != 0) & (df['OriginalPrice'] != 0) & (df['SalesCount'] != 0)]

    conn = psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB', 'airflow'),
        user=os.getenv('POSTGRES_USER', 'airflow'),
        password=os.getenv('POSTGRES_PASSWORD', 'airflow'),
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432')
    )
    cur = conn.cursor()

    cur.execute('''
    CREATE TABLE IF NOT EXISTS raw_products (
        ItemId TEXT,
        Channel TEXT,
        ScrapDate DATE,
        SalesCount INTEGER,
        SalePrice INTEGER,
        OriginalPrice INTEGER,
        Discount INTEGER
    )
    ''')
    conn.commit()

    cur.execute('DELETE FROM raw_products')
    conn.commit()

    for _, row in df.iterrows():
        try:
            cur.execute('''
                INSERT INTO raw_products (ItemId, Channel, ScrapDate, SalesCount, SalePrice, OriginalPrice, Discount)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (
                row['ItemId'],
                row['Channel'],
                row['ScrapDate'],
                row['SalesCount'],
                row['SalePrice'],
                row['OriginalPrice'],
                row['Discount']
            ))
        except Exception as e:
            print(f"[InsertError] Failed to insert row: {row.to_dict()}\nError: {e}")
            continue

    conn.commit()
    cur.close()
    conn.close()

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

dag = DAG(
    dag_id='dqc_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    description='ETL DAG for cleaning e-commerce product data'
)

load_csv = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    dag=dag
)

clean_data = PostgresOperator(
    task_id='clean_data_sql',
    postgres_conn_id='postgres_markethac',
    sql='sql/cleaning_script.sql',
    dag=dag
)

load_csv >> clean_data
