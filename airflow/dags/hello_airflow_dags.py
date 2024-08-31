from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

#DAG 정의
default_args = {
    'owner': 'howdi2000',
    'depends_on_past': False,
    'start_date': datetime(2024, 09, 01),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hello_airflow',
    default_args=default_args,
    description='A simple hello airflow DAG',
    schedule_interval=timedelta(days=1),
)

sentence = "Hello Airflow!.  welcome to the world of Airflow."

def print_word(word):
    print(word)

prev_task = None
for i, word in enumerate(sentence.split()):
    task = PythonOperator(
        task_id=f'print_word_{i}',
        python_callable=print_word,
        op_args={'word': word},
        dag=dag
    )
    if prev_task:
        prev_task >> task
    prev_task = task