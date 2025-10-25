import os
import pandas as pd
from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
import io
from datetime import datetime, timedelta

def load_minio_to_snowflake_direct():
    """Direct MinIO to Snowflake without S3 integration"""
    
    # MinIO configuration
    minio_client = Minio(
        "minio:9000",
        access_key="admin",
        secret_key="password123",
        secure=False
    )
    
    # Snowflake connection
    conn = snowflake.connector.connect(
        user="ASHWIN2001",
        password="AshAna@258643003781",
        account="sz09842.central-india.azure",
        warehouse="COMPUTE_WH",
        database="STOCKS_MDS",
        schema="COMMON"
    )
    
    # Process files from MinIO
    objects = minio_client.list_objects("bronze-transactions", recursive=True)
    
    for obj in objects:
        if obj.object_name.endswith('.json'):  # or .csv
            print(f"Processing: {obj.object_name}")
            
            try:
                # Download and read file directly from MinIO
                response = minio_client.get_object("bronze-transactions", obj.object_name)
                file_content = response.read().decode('utf-8')
                
                # Convert to DataFrame (adjust based on your data format)
                if obj.object_name.endswith('.json'):
                    df = pd.read_json(io.StringIO(file_content))
                else:  # CSV
                    df = pd.read_csv(io.StringIO(file_content))
                
                print(f"DataFrame shape: {df.shape}")
                
                # Write directly to Snowflake table
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=df,
                    table_name="bronze_stock_quotes_raw",
                    schema="COMMON",
                    database="STOCKS_MDS"
                )
                
                print(f"✅ Loaded {nrows} rows from {obj.object_name}")
                
            except Exception as e:
                print(f"❌ Error processing {obj.object_name}: {str(e)}")
            finally:
                response.close()
                response.release_conn()
    
    conn.close()

# DAG definition
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 9, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "minio_to_snowflake_direct",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:

    load_task = PythonOperator(
        task_id="load_minio_to_snowflake",
        python_callable=load_minio_to_snowflake_direct,
    )
