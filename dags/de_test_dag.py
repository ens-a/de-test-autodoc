import pandas as pd
import numpy as np
from pandahouse import to_clickhouse, read_clickhouse
from datetime import datetime

from airflow import DAG

from airflow.operators.python import PythonOperator
 
CALCULATION_DATE="{{ ds }}"

connection = {'host': 'http://clickhouse:8123',
              'database': 'default'}

def download_table(table_name: str, date: str) -> pd.DataFrame:
    
    query = f"""SELECT * 
                FROM {table_name} 
                WHERE toStartOfMonth(Date) = '{date}'"""
    df = read_clickhouse(query , connection=connection)

    return df
    
def build_report(date):
    
    df_sales = download_table('sales', date)

    df_stocks = download_table('stocks', date)

    df_stocks.sort_values('Stock', inplace=True)
    df_stocks.drop_duplicates(['Date', 'article_id'], keep='first', inplace=True)
    df_stocks = df_stocks[df_stocks['Stock'] > 0]

    df_positive_stock = df_stocks.groupby(['article_id']).agg(**{'days': ('Stock', 'count')}).reset_index()


    df_sales = df_sales[df_sales['Qty'] > 0]
    df_sales_gp = df_sales.groupby(['Country', 'article_id']).agg(**{'Qty': ('Qty', 'sum')}).reset_index()

    df_result = df_sales_gp.merge(df_positive_stock, on='article_id', how='left')
    df_result['avg_sales'] = np.where(df_result['days'].isna(), None, df_result['Qty']/df_result['days'])
    df_result['avg_sales'] = df_result['avg_sales'].astype(float)

    df_result['Date'] = pd.to_datetime(date)


    df_result = df_result[['Date',
                        'Country',
                        'article_id',
                        'Qty',
                        'days',
                        'avg_sales']]

    

    rows = to_clickhouse(df_result, table='report', indx=False, connection=connection)

    print(f'Data for the date {date} has been proccesed: {rows}')

default_args = {
	'owner': 'airflow',
	'email': ['anatoly.ens@yandex.ru'],
	'email_on_failure': True
}

with DAG(
    dag_id='de_test_dag',
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    schedule_interval='@monthly',
    catchup=True
) as dag:

    build_report = PythonOperator(
        task_id='build_report',
        python_callable=build_report,
        op_kwargs=dict(date=CALCULATION_DATE)
    )

    build_report
