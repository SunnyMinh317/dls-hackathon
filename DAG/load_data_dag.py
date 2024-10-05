from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
}
 
# GCS bucket and prefix
bucket_name = 'us-central1-composer3demo-13df5625-bucket'
prefix = 'team-1/tmp/'
 
# Define the DAG
with DAG(
    'load_data_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
 
    # Task to load customer data
    load_customer_task = GCSToBigQueryOperator(
        task_id='load_customer_data',
        bucket=bucket_name,
        source_objects=[f'{prefix}validated_customer_*.txt'],
        destination_project_dataset_table='composer-demo-2.dls_hackathon_dlk_1.Customer',
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        write_disposition='WRITE_APPEND',
        autodetect=False,
        schema_fields=[
            {'name': 'Customer_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Phone', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Address', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'City_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Age', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Gender', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Income', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Customer_Segment', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Valid_flag', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'Snapshot_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Input_feed', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
    )
 
    # Task to load transaction data
    load_transaction_task = GCSToBigQueryOperator(
        task_id='load_transaction_data',
        bucket=bucket_name,
        source_objects=[f'{prefix}validated_transaction_*.txt'],
        destination_project_dataset_table='composer-demo-2.dls_hackathon_dlk_1.Transaction',
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        write_disposition='WRITE_APPEND',
        autodetect=False,
        schema_fields=[
            {'name': 'Transaction_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Customer_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Product_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Time', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Quantity', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Price', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
            {'name': 'Feedback', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Shipping_Method', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Payment_Method', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Order_Status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Ratings', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
            {'name': 'Valid_flag', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'Input_feed', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
    )
 
    # Task to load city data
    load_city_task = GCSToBigQueryOperator(
        task_id='load_city_data',
        bucket=bucket_name,
        source_objects=[f'{prefix}validated_city.txt'],
        destination_project_dataset_table='composer-demo-2.dls_hackathon_dlk_1.City',
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        write_disposition='WRITE_APPEND',
        autodetect=False,
        schema_fields=[
            {'name': 'City_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'City', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Input_feed', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
    )
 
    # Task to load product data
    load_product_task = GCSToBigQueryOperator(
        task_id='load_product_data',
        bucket=bucket_name,
        source_objects=[f'{prefix}validated_product.txt'],
        destination_project_dataset_table='composer-demo-2.dls_hackathon_dlk_1.Product',
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        write_disposition='WRITE_APPEND',
        autodetect=False,
        schema_fields=[
            {'name': 'Product_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Product_Name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Product_Category', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Product_Brand', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Product_Type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Input_feed', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
    )

    # wait_for_validate_data = ExternalTaskSensor(
    # task_id="wait_for_validate_data_dag",
    # external_dag_id="validate_data_dag",  # DAG 2 ID
    # external_task_id="validate_and_save_task",  # Waits for the entire DAG to complete
    # timeout=300,  # Adjust timeout if necessary
    # poke_interval=30,  # Time in seconds between each check
    # )

    trigger_dag4 = TriggerDagRunOperator(
        task_id="trigger_archive_dag",
        trigger_dag_id="archive_dag",  # ID of DAG 4
        wait_for_completion=True,  # Optional, waits for the triggered DAG to complete
    )
 
    # Task dependencies
    # wait_for_validate_data >> load_city_task >> load_product_task >> load_customer_task >> load_transaction_task
    load_city_task >> load_product_task >> load_customer_task >> load_transaction_task >> trigger_dag4