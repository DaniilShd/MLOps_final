from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import sys
sys.path.insert(0, '/opt/airflow/plugins')
from transform_script import transform

default_args = {
    'owner': 'Daniil',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extract_data(**kwargs):
    df = pd.read_csv('/opt/airflow/data/profit_table.csv')
    return df


def transform_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='extract_data')

    # Преобразуем execution_date в строку перед передачей
    execution_date = kwargs['logical_date']  # Используем logical_date вместо устаревшего execution_date
    date_str = execution_date.strftime('%Y-%m-%d')

    return transform(profit_table=df, date=date_str)  # Передаем строку вместо DateTime


def load_data(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='transform_data')

    # Добавляем новые данные без перезаписи
    try:
        existing = pd.read_csv('/opt/airflow/data/flags_activity.csv')
        result = pd.concat([existing, result]).drop_duplicates()
    except FileNotFoundError:
        pass

    result.to_csv('/opt/airflow/data/flags_activity.csv', index=False)


with DAG(
        'customer_activity_analysis',
        default_args=default_args,
        schedule_interval='0 0 5 * *',
        catchup=False
) as dag:
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True
    )

    extract_task >> transform_task >> load_task