from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


src_location = ""
dest_location = ""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
        'archive_dag',
        schedule_interval=None,
        default_args=default_args
    ) as dag:

    # wait_for_load_data = ExternalTaskSensor(
    #     task_id="wait_for_load_data",
    #     external_dag_id="load_data_dag",  # DAG 1 ID
    #     external_task_id="load_transaction_task",  # Waits for the entire DAG to complete
    #     timeout=300,  # Adjust timeout if necessary
    #     poke_interval=30,  # Time in seconds between each check
    # )

    trigger_dag5 = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="transform_dag",  # ID of DAG 4
        wait_for_completion=False,  # Optional, waits for the triggered DAG to complete
    )

    copy_files = BashOperator(
        task_id='copy_files',
        bash_command=f'gsutil -m cp {src_location} {dest_location}'
    )

    copy_files >> trigger_dag5