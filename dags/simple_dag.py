from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from utils import get_cur_data, change_data

URL = 'http://www.cbr.ru/scripts/XML_daily.asp'
raw_data_path = "/opt/airflow/data/current_data_{{ ds }}.csv"
result_data_path = "/opt/airflow//data/result_data_{{ ds }}.csv"

@dag("airflow_example_my_dag", start_date=days_ago(0), schedule='@daily', catchup=False)
def taskflow():
    task_load_data = PythonOperator(
        task_id='load_data',
        python_callable=get_cur_data,
        op_kwargs={'url': URL, 'path': raw_data_path}

    )
    task_change_data = PythonOperator(
        task_id="change_data",
        python_callable=change_data,
        op_kwargs={'path_in': raw_data_path, 'path_out': result_data_path}
    )

    task_load_data >> task_change_data

taskflow()