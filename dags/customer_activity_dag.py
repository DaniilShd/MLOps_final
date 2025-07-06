from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os
from tqdm import tqdm
from tempfile import NamedTemporaryFile

default_args = {
    'owner': 'Daniil',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['ds_const@mail.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def transform(profit_table, date):
    print(f"Received date type: {type(date)}, value: {date}")
    """ Собирает таблицу флагов активности по продуктам
        на основании прибыли и количеству совершёных транзакций

        :param profit_table: таблица с суммой и кол-вом транзакций
        :param date: дата расчёта флагов активности

        :return df_tmp: pandas-датафрейм флагов за указанную дату
    """

    # Преобразуем date в pandas.Timestamp, если это еще не сделано
    if not isinstance(date, pd.Timestamp):
        date = pd.to_datetime(date)


    start_date = date - pd.DateOffset(months=2)
    end_date = date + pd.DateOffset(months=1)
    date_list = pd.date_range(
        start=start_date, end=end_date, freq='M'
    ).strftime('%Y-%m-01')

    df_tmp = (
        profit_table[profit_table['date'].isin(date_list)]
        .drop('date', axis=1)
        .groupby('id')
        .sum()
    )

    # Сохраните данные для отладки
    debug_path = '/opt/airflow/data/debug_data.csv'
    df_tmp.to_csv(debug_path, index=False)
    print(f"Debug data saved to {debug_path}")

    product_list = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
    for product in tqdm(product_list):
        df_tmp[f'flag_{product}'] = (
            df_tmp.apply(
                lambda x: x[f'sum_{product}'] != 0 and x[f'count_{product}'] != 0,
                axis=1
            ).astype(int)
        )

    df_tmp = df_tmp.filter(regex='flag').reset_index()

    return df_tmp



def extract_data(**kwargs):
    """Извлечение данных из CSV файла"""
    try:
        df = pd.read_csv('/opt/airflow/data/profit_table.csv')
        print(f"Successfully loaded data with shape: {df.shape}")
        if df.memory_usage().sum() > 45000:
            print("Data too large for XCom")
        with NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            df.to_parquet(tmp.name)
            kwargs['ti'].xcom_push(key='data_path', value=tmp.name)
        return df
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise



def transform_data(**kwargs):
    """Преобразование данных с использованием готовой функции transform"""
    ti = kwargs['ti']
    tmp_path = ti.xcom_pull(task_ids='extract_data', key='data_path')

    # Читаем из временного файла
    df = pd.read_parquet(tmp_path)

    # Сохраните данные для отладки
    # debug_path = '/opt/airflow/data/debug_data.csv'
    # df.to_csv(debug_path, index=False)
    # print(f"Debug data saved to {debug_path}")

    # Обработка данных


    # Рассчитываем даты для 3 месяцев (X, X-1, X-2)
    # date_str = execution_date.strftime('%Y-%m-%d')
    # print(f"Processing data for date: {date_str}")

    try:
        # Вызываем функцию преобразования от data science команды
        result = transform(df, kwargs['logical_date'].strftime('%Y-%m-%d'))
        print(f"Transformed data shape: {result.shape}")
        return result
    except Exception as e:
        print(f"Error in transform: {str(e)}")
        raise


def load_data(**kwargs):
    """Загрузка результатов с добавлением новых данных без перезаписи"""
    ti = kwargs['ti']
    execution_date = kwargs['logical_date']
    date_str = execution_date.strftime('%Y-%m-%d')

    # Получаем преобразованные данные
    new_data = ti.xcom_pull(task_ids='transform_data')
    output_path = '/opt/airflow/data/flags_activity.csv'

    try:
        # Пытаемся загрузить существующие данные
        if os.path.exists(output_path):
            existing_data = pd.read_csv(output_path)

            # Удаляем старые записи для этой даты, если они есть
            existing_data = existing_data[existing_data['analysis_date'] != date_str]

            # Объединяем с новыми данными
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        else:
            combined_data = new_data

        # Сохраняем с режимом перезаписи, но сохраняя исторические данные
        combined_data.to_csv(output_path, index=False)
        print(f"Data successfully saved to {output_path}")

    except Exception as e:
        print(f"Error saving data: {str(e)}")
        raise


with DAG(
        'customer_activity_analysis',
        default_args=default_args,
        description='ETL process for customer activity flags',
        schedule_interval='0 0 5 * *',  # 5-го числа каждого месяца
        catchup=False,
        tags=['analytics', 'customer_activity'],
) as dag:
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task