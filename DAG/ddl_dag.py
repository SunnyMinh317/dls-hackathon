from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
 
# Default arguments
default_args = {
    "start_date": datetime.today() - timedelta(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
 
# Define the DAG
with DAG(
    dag_id="create_tables_dag",
    catchup=False,
    schedule_interval=None,  # or adjust based on your requirement
    default_args=default_args,
) as dag:
 
    start = DummyOperator(task_id="start")
 
    create_customer_table = BashOperator(
        task_id="create_customer_table",
        bash_command='bq query --use_legacy_sql=false "CREATE TABLE IF NOT EXISTS composer-demo-2.dls_hackathon_dlk_1.Customer (Customer_ID STRING, Name STRING, Email STRING, Phone STRING, Address STRING, City_ID STRING, Age INT, Gender STRING, Income STRING, Customer_Segment STRING, Valid_flag BOOLEAN, Snapshot_date DATE, Input_feed STRING, PRIMARY KEY (Customer_ID) NOT ENFORCED) PARTITION BY Snapshot_Date;"',
    )
 
    create_city_table = BashOperator(
        task_id="create_city_table",
        bash_command='bq query --use_legacy_sql=false "CREATE TABLE IF NOT EXISTS composer-demo-2.dls_hackathon_dlk_1.City (City_ID STRING, City STRING, State STRING, Country STRING, Input_feed STRING, PRIMARY KEY (City_ID) NOT ENFORCED);"',
    )
 
    create_product_table = BashOperator(
        task_id="create_product_table",
        bash_command='bq query --use_legacy_sql=false "CREATE TABLE IF NOT EXISTS composer-demo-2.dls_hackathon_dlk_1.Product (Product_ID STRING, Product_Name STRING, Product_Category STRING, Product_Brand STRING, Product_Type STRING, Input_feed STRING, PRIMARY KEY (Product_ID) NOT ENFORCED);"',
    )
 
    create_transaction_table = BashOperator(
        task_id="create_transaction_table",
        bash_command='bq query --use_legacy_sql=false "CREATE TABLE IF NOT EXISTS composer-demo-2.dls_hackathon_dlk_1.Transaction (Transaction_ID STRING, Customer_ID STRING, Product_ID STRING, Date DATE, Time DATETIME, Quantity INT, Price NUMERIC, Feedback STRING, Shipping_Method STRING, Payment_Method STRING, Order_Status STRING, Ratings INT, Valid_flag BOOLEAN, Input_feed STRING, PRIMARY KEY (Transaction_ID) NOT ENFORCED, FOREIGN KEY (Customer_ID) REFERENCES dls_hackathon_dlk_1.Customer(Customer_ID) NOT ENFORCED, FOREIGN KEY (Product_ID) REFERENCES dls_hackathon_dlk_1.Product(Product_ID) NOT ENFORCED) PARTITION BY Date;"',
    )

    # Task to trigger DAG 2 after DAG 1 finishes
    trigger_dag2 = TriggerDagRunOperator(
        task_id="trigger_validate_data_dag",
        trigger_dag_id="validate_data_dag",  # ID of DAG 2
        wait_for_completion=False,  # Optional, waits for the triggered DAG to complete
    )
 
    # Task dependencies
    start >> create_customer_table >> create_city_table >> create_product_table >> create_transaction_table >> trigger_dag2