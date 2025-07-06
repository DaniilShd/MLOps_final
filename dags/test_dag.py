from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Daniil',
    'depends_on_past': False,
    'email': ['ds_const@mail.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hello_world',
    description='Hello World DAG',
    schedule_interval='0 12 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    template_searchpath=['/opt/airflow/dags/scripts']  # Добавляем путь для поиска скриптов
) as dag:

    hello_operator = BashOperator(
        task_id='hello_task',
        bash_command='echo "Hello from Airflow inside container"'
    )

    # Используем параметр env для передачи переменных окружения
    hello_file_operator = BashOperator(
        task_id='hello_file_task',
        bash_command='{{"sh /opt/airflow/dags/scripts/test.sh"}}',
        params={'jinja_rendered_template': False},
        env={'PATH': '/usr/local/bin:/usr/bin:/bin'}  # Убедимся, что PATH установлен
    )

    skip_operator = BashOperator(
        task_id='skip_task',
        bash_command='exit 99'
    )

    # Определяем зависимости
    hello_operator >> hello_file_operator >> skip_operator