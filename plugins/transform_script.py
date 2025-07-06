import pandas as pd
from tqdm import tqdm
import os


def transform(profit_table, date):
    print(f"Received date type: {type(date)}, value: {date}")
    """ Собирает таблицу флагов активности по продуктам
        на основании прибыли и количеству совершёных транзакций

        :param profit_table: таблица с суммой и кол-вом транзакций
        :param date: дата расчёта флагоа активности

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


if __name__ == "__main__":
    # Получаем абсолютный путь к файлу
    BASE_DIR = "/home/daniil/Learn/CV/MLOPs/final_assignment"
    input_path = os.path.join(BASE_DIR, "data", "profit_table.csv")
    output_path = os.path.join(BASE_DIR, "data", "flags_activity.csv")

    profit_data = pd.read_csv(input_path )
    flags_activity = transform(profit_data, '2024-04-01')
    flags_activity.to_csv('data/flags_activity.csv', index=False)