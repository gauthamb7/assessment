from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

from resources.util_read_insert_data import load_excel_to_db, process_notebook_and_generate_csv

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 8),
}

with DAG('data_pipeline_dag', default_args=default_args, schedule_interval=None) as dag:
    ingest_messages = PythonOperator(
        task_id='task_ingest_messages_data',
        python_callable=load_excel_to_db,
        op_kwargs={
                    'file_path': '/opt/airflow/dataset/Senior Data Engineer Task Assignment.xlsx',
                    'postgres_conn_id': 'external_postgres',
                    'table_name': 'l0_messages',
                    'sheet_name': 'Messages',
                    'notebook_path': '/opt/airflow/dataset/cleanup_messages.ipynb',
                    'output_notebook_path': '/opt/airflow/dataset/output_cleanup_messages.ipynb',
                    'output_csv_path': '/opt/airflow/dataset/cleanup_messages.csv'
                },
    )

    ingest_statuses = PythonOperator(
        task_id='task_ingest_statuses_data',
        python_callable=load_excel_to_db,
        op_kwargs={
                    'file_path': '/opt/airflow/dataset/Senior Data Engineer Task Assignment.xlsx',
                    'postgres_conn_id': 'external_postgres',
                    'table_name': 'l0_statuses',
                    'sheet_name': 'Statuses',
                    'notebook_path': '/opt/airflow/dataset/cleanup_statuses.ipynb',
                    'output_notebook_path': '/opt/airflow/dataset/output_cleanup_statuses.ipynb',
                    'output_csv_path': '/opt/airflow/dataset/cleanup_statuses.csv'
                },
    )

    notebook_transform_visualise = PythonOperator(
        task_id='task_transform_visualise',
        python_callable=process_notebook_and_generate_csv,
        op_kwargs={
                    'notebook_path': '/opt/airflow/dataset/transformation_visualisation.ipynb',
                    'output_notebook_path': '/opt/airflow/dataset/output_transformation_visualisation.ipynb',
                },
    )

    ingest_messages >> ingest_statuses >> notebook_transform_visualise