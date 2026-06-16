from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# IMPORTANTE:
# Ajusta esta importación según dónde esté save_raw_data
from batch_generator import save_raw_data

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

# Define DAG
dag = DAG(
    'extract_raw_data_pipeline',
    default_args=default_args,
    description='DataDriven Main Pipeline',
    schedule_interval='0 7 * * *',
    start_date=datetime(2024, 9, 22),
    catchup=False,
)

extract_raw_data_task = PythonOperator(
    task_id='extract_raw_data',
    python_callable=save_raw_data,
    dag=dag,
)

create_raw_schema_task = SQLExecuteQueryOperator(
    task_id='create_raw_schema',
    conn_id='postgres_conn',
    sql="""
        CREATE SCHEMA IF NOT EXISTS driven_raw;
    """,
    dag=dag,
)

create_raw_table_task = SQLExecuteQueryOperator(
    task_id='create_raw_table',
    conn_id='postgres_conn',
    sql="""
        CREATE TABLE IF NOT EXISTS driven_raw.raw_batch_data (
            person_name VARCHAR(100),
            user_name VARCHAR(100),
            email VARCHAR(100),
            personal_number VARCHAR(100),
            birth_date VARCHAR(100),
            address VARCHAR(255),
            phone VARCHAR(100),
            mac_address VARCHAR(100),
            ip_address VARCHAR(100),
            iban VARCHAR(100),
            accessed_at TIMESTAMP,
            session_duration INT,
            download_speed INT,
            upload_speed INT,
            consumed_traffic INT,
            unique_id VARCHAR(100)
        );
    """,
    dag=dag,
)


load_raw_data_task = SQLExecuteQueryOperator(
    task_id='load_raw_data',
    conn_id='postgres_conn',
    sql="""
        COPY driven_raw.raw_batch_data (
            person_name,
            user_name,
            email,
            personal_number,
            birth_date,
            address,
            phone,
            mac_address,
            ip_address,
            iban,
            accessed_at,
            session_duration,
            download_speed,
            upload_speed,
            consumed_traffic,
            unique_id
        )
        FROM '/opt/airflow/data/raw_data.csv'
        DELIMITER ','
        CSV HEADER;
    """,
    dag=dag,
)


run_dbt_staging_task = BashOperator(
    task_id='run_dbt_staging',
    bash_command="""
        set -x
        cd /opt/airflow/dbt
        dbt run --select tag:staging
    """,
    dag=dag,
)


run_dbt_trusted_task = BashOperator(
    task_id='run_dbt_trusted',
    bash_command="""
        set -x
        cd /opt/airflow/dbt
        dbt run --select tag:trusted
    """,
    dag=dag,
)

[extract_raw_data_task, create_raw_schema_task] >> create_raw_table_task

create_raw_table_task >> load_raw_data_task

load_raw_data_task >> run_dbt_staging_task

run_dbt_staging_task >> run_dbt_trusted_task