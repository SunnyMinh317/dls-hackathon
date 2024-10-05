from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

project = 'composer-demo-2'
dataset = 'dls_hackathon_dlk_1'

# Define the DAG
with DAG(
    dag_id='transform_dag',
    default_args=default_args,
    description='Create and populate BigQuery tables if they do not exist',
    schedule_interval=None,  # Change this to your desired schedule
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task to create and populate dnorm_customer table
    create_dnorm_customer = BigQueryInsertJobOperator(
        task_id='create_or_update_dnorm_customer',
        configuration={
            "query": {
                "query": f"""
                BEGIN
                DECLARE table_exists BOOL;

                SET table_exists = (
                  SELECT COUNT(*) > 0
                  FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
                  WHERE table_name = 'dnorm_customer'
                );

                IF NOT table_exists THEN
                  CREATE TABLE IF NOT EXISTS `{project}.{dataset}.dnorm_customer` AS
                  SELECT customer.*, city.city, city.state, city.country
                  FROM (SELECT * FROM `{project}.{dataset}.Customer` cust WHERE cust.valid_flag = True) AS customer 
                  LEFT JOIN `{project}.{dataset}.City` city
                  ON customer.city_id = city.city_id;
                ELSE
                  TRUNCATE TABLE `{project}.{dataset}.dnorm_customer`;
                  INSERT INTO `{project}.{dataset}.dnorm_customer`
                  SELECT customer.*, city.city, city.state, city.country
                  FROM (SELECT * FROM `{project}.{dataset}.Customer` cust WHERE cust.valid_flag = True) AS customer 
                  LEFT JOIN `{project}.{dataset}.City` city
                  ON customer.city_id = city.city_id;
                END IF;
                END;
                """,
                "useLegacySql": False,
            }
        },
        location='US'  # Set your dataset location
    )

    # Task to create and populate obt_transaction table
    create_obt_transaction = BigQueryInsertJobOperator(
        task_id='create_or_update_obt_transaction',
        configuration={
            "query": {
                "query": f"""
                BEGIN
                DECLARE table_exists BOOL;

                SET table_exists = (
                  SELECT COUNT(*) > 0
                  FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
                  WHERE table_name = 'obt_transaction'
                );

                IF NOT table_exists THEN
                  CREATE TABLE IF NOT EXISTS `{project}.{dataset}.obt_transaction` AS
                  SELECT transaction.*, product.product_name, product.product_category, product.product_brand, product.product_type, customer.name, customer.email, customer.phone, customer.address, customer.age, customer.gender, customer.income, customer.customer_segment, customer.city, customer.state, customer.country
                  FROM (SELECT * FROM {project}.{dataset}.Transaction WHERE valid_flag = True) trans 
                  LEFT JOIN {project}.{dataset}.Product product
                  ON trans.product_id = product.product_id
                  LEFT JOIN {project}.{dataset}.dnorm_customer customer
                  ON trans.customer_id = customer.customer_id;
                ELSE
                  TRUNCATE TABLE `{project}.{dataset}.obt_transaction`;
                  INSERT INTO `{project}.{dataset}.obt_transaction`
                  SELECT transaction.*, product.product_name, product.product_category, product.product_brand, product.product_type, customer.name, customer.email, customer.phone, customer.address, customer.age, customer.gender, customer.income, customer.customer_segment, customer.city, customer.state, customer.country
                  FROM (SELECT * FROM {project}.{dataset}.Transaction WHERE valid_flag = True) trans 
                  LEFT JOIN {project}.{dataset}.Product product
                  ON trans.product_id = product.product_id
                  LEFT JOIN {project}.{dataset}.dnorm_customer customer
                  ON trans.customer_id = customer.customer_id;
                END IF;
                END;
                """,
                "useLegacySql": False,
            }
        },
        location='US'  # Set your dataset location
    )

    # Set task dependencies
    create_dnorm_customer >> create_obt_transaction