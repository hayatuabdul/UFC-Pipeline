from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from ETL.ETL import extract_data, transform_load

default_args = {
     'owner': 'FlowBoss',
     'retries' : 5,
     'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='UFC_PipelineV5',
    default_args=default_args,
    description='UFC Pipeline with Airflow',
    start_date=datetime(2023, 11, 7),
    schedule_interval='@daily'
) as dag:

    def extract_data_task():
        data = extract_data()
        return data

    def transform_load_task(ti):
        data = ti.xcom_pull(task_ids='extract_data_task')
        transform_load(data)

    extract_data_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data_task,
        provide_context=True,
    )

    transform_load_task = PythonOperator(
        task_id='transform_load_task',
        python_callable=transform_load_task,
        provide_context=True,
    )

    extract_data_task >> transform_load_task
