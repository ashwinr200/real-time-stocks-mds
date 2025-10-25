import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET = "bronze-transactions"
LOCAL_DIR = "/tmp/minio_downloads"

# UPDATE THESE WITH YOUR ACTUAL SNOWFLAKE CREDENTIALS
SNOWFLAKE_USER = "ASHWIN2001"  # Replace with your actual username
SNOWFLAKE_PASSWORD = "AshAna@258643003781"  # Replace with your actual password
SNOWFLAKE_ACCOUNT = "sz09842.central-india.azure"  # Replace with your actual account
SNOWFLAKE_ROLE ="ACCOUNTADMIN"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DB = "STOCKS_MDS"
SNOWFLAKE_SCHEMA = "COMMON"

def download_from_minio():
    try:
        os.makedirs(LOCAL_DIR, exist_ok=True)
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        
        # Test MinIO connection
        buckets = s3.list_buckets()
        print(f"✅ Connected to MinIO. Buckets: {[b['Name'] for b in buckets['Buckets']]}")
        
        objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        local_files = []
        
        if not objects:
            print("❌ No files found in bucket")
            return []
            
        for obj in objects:
            key = obj["Key"]
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(BUCKET, key, local_file)
            print(f"✅ Downloaded {key} -> {local_file}")
            local_files.append(local_file)
            
        print(f"📁 Total files downloaded: {len(local_files)}")
        return local_files
        
    except Exception as e:
        print(f"❌ MinIO download error: {str(e)}")
        return []

def load_to_snowflake(**kwargs):
    local_files = kwargs['ti'].xcom_pull(task_ids='download_minio')
    
    if not local_files:
        print("❌ No files to load.")
        return

    try:
        # Test Snowflake connection
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DB,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE
        )
        print("✅ Connected to Snowflake successfully")
        
        cur = conn.cursor()
        
        # Test if table exists
        try:
            cur.execute("SELECT 1 FROM STOCKS_MDS.COMMON.bronze_stock_quotes_raw LIMIT 1")
            print("✅ Table exists: STOCK_MDS.COMMON.bronze_stock_quotes_raw")
        except:
            print("❌ Table STOCK_MDS.COMMON.bronze_stock_quotes_raw does not exist or inaccessible")
            raise

        # Upload files to stage
        for f in local_files:
            try:
                # Use fully qualified table stage
                put_command = f"PUT 'file://{f}' @STOCKS_MDS.COMMON.%bronze_stock_quotes_raw AUTO_COMPRESS=FALSE"
                print(f"🔄 Executing: {put_command}")
                cur.execute(put_command)
                print(f"✅ Uploaded {f} to Snowflake stage")
            except Exception as e:
                print(f"❌ PUT failed for {f}: {str(e)}")
                continue

        # Copy data into table
        copy_command = """
        COPY INTO STOCKS_MDS.COMMON.bronze_stock_quotes_raw
        FROM @STOCKS_MDS.COMMON.%bronze_stock_quotes_raw
        FILE_FORMAT = (TYPE=JSON)
        """
        print("🔄 Executing COPY command...")
        cur.execute(copy_command)
        result = cur.fetchall()
        print(f"✅ COPY executed successfully. Results: {result}")

        cur.close()
        conn.close()
        
        # Cleanup local files
        for f in local_files:
            try:
                os.remove(f)
                print(f"🧹 Cleaned up: {f}")
            except:
                pass
                
    except Exception as e:
        print(f"❌ Snowflake load error: {str(e)}")
        raise

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "minio_to_snowflake",
    default_args=default_args,
    schedule_interval="*/5 * * * *",  # Changed to 5 minutes for testing
    catchup=False,
    max_active_runs=1,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    task1 >> task2
