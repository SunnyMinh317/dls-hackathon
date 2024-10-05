from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from google.cloud import storage
import datetime
import pandas as pd
 
# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}
 
# GCS Bucket and file path
GCS_BUCKET = 'us-central1-composer3demo-13df5625-bucket'
 
def list_files_in_gcs(bucket_name, prefix):
    """ List all files in GCS bucket with the given prefix """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs]
 
def read_file_from_gcs(bucket_name, file_path):
    """ Đọc dữ liệu từ GCS """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_string().decode('utf-8')
 
    # Chuyển đổi nội dung file .txt thành DataFrame
    from io import StringIO
    df = pd.read_csv(StringIO(content), delimiter='|')
   
    return df
 
def validate_customer_data(df, file_path):
    # Ensure columns are of correct type
    df['Email'] = df['Email'].astype(str)
    df['Age'] = df['Age'].fillna(0)
    df['Age'] = pd.to_numeric(df['Age'], errors='coerce').astype(int)
    df['Gender'] = df['Gender'].astype(str)
    df['Income'] = df['Income'].astype(str)
 
    # Check Email format using regex
    email_regex = r'^\w+@\w+\.\w+$'
    df['valid_email'] = df['Email'].str.match(email_regex)
 
    # Check Age is between 0 and 150
    df['valid_age'] = df['Age'].between(0, 150, inclusive='both')
 
    # Check Gender is in set ['Male', 'Female']
    df['valid_gender'] = df['Gender'].isin(['Male', 'Female'])
 
    # Check Income is greater than 0
    df['valid_income'] = df['Income'].isin(['Low', 'Medium', 'High'])
 
    # Combine all validation flags into one
    df['valid_flag'] = df[['valid_email', 'valid_age', 'valid_gender', 'valid_income']].all(axis=1)
 
    date_str = (file_path.split("/")[-1].split(".")[0].split("_")[-1] + '01')

    year = date_str[:4]
    month = date_str[4:6]
    day = date_str[6:]

    # Create a datetime object from the extracted components
    formatted_date = datetime.datetime(int(year), int(month), int(day))

    # Format the datetime object as "2024:01:01"
    output_format = "%Y:%m:%d"
    formatted_date_str = formatted_date.strftime(output_format)

    df['snapshot_date'] = formatted_date_str
    df['Input_feed'] = file_path.split('/')[-1]
 
    # Drop individual validation columns if needed (optional)
    df.drop(columns=['valid_email', 'valid_age', 'valid_gender', 'valid_income'], inplace=True)
 
    return df
 
def validate_transaction_data(df, file_path):
    # Ensure columns are of correct type
    df['Quantity'] = df['Quantity'].fillna(0)
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').astype(int)
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    
    # Convert the 'date' column to datetime format
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')

    # Format the datetime values as "2024:01:01"
    df['Date'] = df['Date'].dt.strftime('%Y:%m:%d')

    df['valid_quantity'] = df['Quantity'] > 0
    df['valid_price'] = df['Price'] > 0
 
    # Combine all validation flags into one
    df['valid_flag'] = df[['valid_quantity', 'valid_price']].all(axis=1)
 
    df['Input_feed'] = file_path.split('/')[-1]
    # Drop individual validation columns if needed (optional)
    df.drop(columns=['valid_quantity', 'valid_price'], inplace=True)
 
    return df

def handle_city_feed(df, file_path):
    df['Input_feed'] = file_path.split('/')[-1]
    return df

def handle_product_feed(df, file_path):
    df['Input_feed'] = file_path.split('/')[-1]
    return df

def save_validated_data_to_gcs(df, bucket_name, output_path):
    """ Lưu dữ liệu đã được kiểm tra lại vào GCS """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    output_blob = bucket.blob(output_path)
    output_blob.upload_from_string(df.to_csv(index=False), 'text/csv')
 
def validate_and_save():
    """Reads data, validates it with pandas, and saves it back to GCS."""
    prefixes = ['team-1/incoming/customer/', 'team-1/incoming/transaction/', 'team-1/incoming/master/']
    for prefix in prefixes:
        files = list_files_in_gcs(GCS_BUCKET, prefix)
        for file_path in files:
            df = read_file_from_gcs(GCS_BUCKET, file_path)
            if 'customer' in file_path:
                df_validated = validate_customer_data(df, file_path)
            elif 'transaction' in file_path:
                df_validated = validate_transaction_data(df, file_path)
            elif 'city' in file_path:
                df_validated = handle_city_feed(df, file_path)
            elif 'product' in file_path:
                df_validated = handle_product_feed(df, file_path)
 
            # Save validated data back to GCS
            save_validated_data_to_gcs(df_validated, GCS_BUCKET, f'team-1/tmp/validated_{file_path.split("/")[-1]}')
 
# Define the DAG
with DAG(
    'validate_data_dag',
    default_args=default_args,
    description='DAG to validate data from GCS',
    schedule_interval=None,
) as dag:
    

    # wait_for_create_tables = ExternalTaskSensor(
    #     task_id="wait_for_create_tables_dag",
    #     external_dag_id="create_tables_dag",  # DAG 1 ID
    #     external_task_id="create_transaction_table",  # Waits for the entire DAG to complete
    #     timeout=300,  # Adjust timeout if necessary
    #     poke_interval=30,  # Time in seconds between each check
    # )

    validate_and_save_task = PythonOperator(
        task_id='validate_and_save',
        python_callable=validate_and_save,
    )

    # Task to trigger DAG 3 after validation is complete
    trigger_dag3 = TriggerDagRunOperator(
        task_id="trigger_load_data_dag",
        trigger_dag_id="load_data_dag",  # ID of DAG 3
        wait_for_completion=False,  # Optional, waits for the triggered DAG to complete
    )
 
    # wait_for_create_tables >> validate_and_save_task

    validate_and_save_task >> trigger_dag3