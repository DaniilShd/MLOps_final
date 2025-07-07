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

SHARED_TMP_DIR = '/opt/airflow/shared_tmp'
os.makedirs(SHARED_TMP_DIR, exist_ok=True)

def delete_csv_file(file_path):
  """
  Удаляет файл csv по указанному пути.

  :param file_path: Полный путь к файлу Parquet
  """
  try:
      if os.path.exists(file_path):
          os.remove(file_path)
          print(f"Файл {file_path} успешно удалён")
      else:
          print(f"Файл {file_path} не найден")
  except Exception as e:
      print(f"Ошибка при удалении файла {file_path}: {e}")
      raise


def transform(profit_table, date):
    """
        Собирает таблицу флагов активности по продуктам на основе прибыли и количества транзакций.

        Args:
            profit_table (pd.DataFrame): Таблица с данными о суммах и количестве транзакций.
            date: Дата расчёта флагов активности (строка, datetime или pd.Timestamp).

        Returns:
            pd.DataFrame: Датафрейм с флагами активности за указанную дату.
        """
    print(f"Received date type: {type(date)}, value: {date}")

    # Преобразуем date в pd.Timestamp, если это еще не сделано
    if not isinstance(date, pd.Timestamp):
        date = pd.to_datetime(date)

    # Определяем диапазон дат
    start_date = date - pd.DateOffset(months=2)
    end_date = date + pd.DateOffset(months=1)
    date_list = pd.date_range(
        start=start_date, end=end_date, freq='M'
    ).strftime('%Y-%m-01')

    # Фильтруем данные по датам и агрегируем
    df_tmp = (
        profit_table[profit_table['date'].isin(date_list)]
        .drop('date', axis=1)
        .groupby('id')
        .sum()
    )

    # Генерируем флаги активности для каждого продукта
    product_list = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
    for product in tqdm(product_list, desc="Generating flags"):
        df_tmp[f'flag_{product}'] = (
                (df_tmp[f'sum_{product}'] != 0) &
                (df_tmp[f'count_{product}'] != 0)
        ).astype(int)

    # Оставляем только флаги и сбрасываем индекс
    df_tmp = df_tmp.filter(regex='flag_').reset_index()
    # Добавляем дату анализа
    df_tmp['analysis_date'] = date.strftime('%Y-%m-%d')


    return df_tmp

def extract_data(**kwargs):
    """Извлечение данных из CSV файла"""
    try:
        # Читаем данные
        df = pd.read_csv('/opt/airflow/data/profit_table.csv')

        # Сохраняем в общую директорию
        tmp_path = f"{SHARED_TMP_DIR}/extract_{kwargs['run_id']}.csv"
        df.to_csv(tmp_path, index=False)

        # Проверяем запись
        assert os.path.exists(tmp_path), "Файл не был создан"
        print(f"{tmp_path}")
        return tmp_path

    except Exception as e:
        print(f"Ошибка в extract_data: {str(e)}")
        raise


def transform_data(**kwargs):
    """Преобразование данных с использованием готовой функции transform"""
    ti = kwargs['ti']
    try:
        # Получаем путь из предыдущей задачи
        input_path = ti.xcom_pull(task_ids='extract_data')
        print(f"{input_path}")
        if not input_path or not os.path.exists(input_path):
            raise FileNotFoundError(f"Файл {input_path} не найден")

        # Читаем данные
        df = pd.read_csv(input_path)

        delete_csv_file(input_path)

        # получаем дату из параметра dag
        manual_date = kwargs['params'].get('analysis_date')
        date = pd.to_datetime(manual_date) if manual_date else kwargs['logical_date']

        # Обработка
        result = transform(df, date)

        # Сохраняем результат
        output_path = f"{SHARED_TMP_DIR}/transform_{kwargs['run_id']}.csv"
        result.to_csv(output_path, index=False)
        return output_path

    except Exception as e:
        print(f"Ошибка в transform_data: {str(e)}")
        raise


def load_data(**kwargs):
    """Загрузка результатов с добавлением новых данных без перезаписи"""
    ti = kwargs['ti']

    # Получаем путь к временному файлу из предыдущей задачи
    tmp_path = ti.xcom_pull(task_ids='transform_data')
    if not tmp_path:
        raise ValueError("No path received from transform_data task")

    # Основной файл для хранения результатов
    output_path = '/opt/airflow/data/flags_activity.csv'

    try:
        # Читаем новые данные
        new_data = pd.read_csv(tmp_path)
        if new_data.empty:
            print("Warning: Received empty dataframe")
            return

        # Проверяем наличие колонки analysis_date
        if 'analysis_date' not in new_data.columns:
            raise KeyError("Column 'analysis_date' not found in new data")

        current_analysis_date = new_data['analysis_date'].iloc[0]

        # Загружаем и обновляем существующие данные
        if os.path.exists(output_path):
            existing_data = pd.read_csv(output_path)

            # Удаляем записи за текущую дату анализа (если они уже есть)
            mask = existing_data['analysis_date'] != current_analysis_date
            existing_data = existing_data[mask]

            # Объединяем данные
            result = pd.concat([existing_data, new_data], ignore_index=True)
        else:
            result = new_data

        # Сохраняем результат
        result.to_csv(output_path, index=False)
        print(f"Successfully saved {len(result)} records to {output_path}")

    except Exception as e:
        print(f"Error in load_data: {str(e)}")

        # Сохраняем новые данные для диагностики
        error_path = f'/opt/airflow/data/error_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        new_data.to_csv(error_path, index=False)
        print(f"Saved error data to {error_path}")

        raise

    finally:
        # Всегда удаляем временный файл
        if tmp_path and os.path.exists(tmp_path):
            try:
                delete_csv_file(tmp_path)
                print(f"Temporary file {tmp_path} removed")
            except Exception as e:
                print(f"Warning: Could not remove temp file {tmp_path}: {str(e)}")


with DAG(
        'customer_activity_analysis',
        default_args=default_args,
        description='ETL process for customer activity flags',
        schedule_interval='0 0 5 * *',  # 5-го числа каждого месяца
        catchup=False,
        tags=['analytics', 'customer_activity'],
        params={
        'analysis_date': datetime.now().strftime('%Y-%m-%d')  # Значение по умолчанию
    },
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