from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_imports():
    print("=== TESTING IMPORTS ===")
    try:
        import boto3
        print("✅ boto3 imported")
    except Exception as e:
        print(f"❌ boto3 import failed: {e}")
    
    try:
        import snowflake.connector
        print("✅ snowflake.connector imported")
    except Exception as e:
        print(f"❌ snowflake.connector import failed: {e}")
    return "Done"

def test_snowflake_connection():
    print("=== TESTING SNOWFLAKE CONNECTION ===")
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(
            user='ASHWIN2001',
            password='AshAna@258643003781',
            account='sz09842.central-india.azure',
            warehouse='COMPUTE_WH',
            database='STOCKS_MDS',
            schema='COMMON'
        )
        print("✅ Snowflake connection successful")
        
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM bronze_stock_quotes_raw")
        count = cur.fetchone()[0]
        print(f"✅ Table access successful - {count} records")
        
        cur.close()
        conn.close()
        return "SUCCESS"
    except Exception as e:
        print(f"❌ Snowflake error: {e}")
        return f"FAILED: {e}"

def test_minio_connection():
    print("=== TESTING MINIO CONNECTION ===")
    try:
        import boto3
        s3 = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='admin',
            aws_secret_access_key='password123',
            verify=False
        )
        
        response = s3.list_objects_v2(Bucket='bronze-transactions')
        if 'Contents' in response:
            print(f"✅ MinIO connection successful - {len(response['Contents'])} files")
        else:
            print("✅ MinIO connection successful - No files")
        return "SUCCESS"
    except Exception as e:
        print(f"❌ MinIO error: {e}")
        return f"FAILED: {e}"

with DAG(
    "debug_snowflake",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test_imports_task = PythonOperator(
        task_id="test_imports",
        python_callable=test_imports,
    )

    test_snowflake_task = PythonOperator(
        task_id="test_snowflake_connection",
        python_callable=test_snowflake_connection,
    )

    test_minio_task = PythonOperator(
        task_id="test_minio_connection", 
        python_callable=test_minio_connection,
    )

    test_imports_task >> [test_snowflake_task, test_minio_task]
