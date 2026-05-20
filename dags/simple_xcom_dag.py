from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import random

#un return va dans le Xcom blackboard
def function_with_return0():
    return random.uniform(a=0, b=1)

#
def function_with_return(task_instance):
    x=3+2+random.uniform(a=0, b=1)
    x={
            "hello": "world",
            "bonjour": "le monde"
        }
    task_instance.xcom_push(
        key="my_xcom_value",
        value=x
    )

def read_data_from_xcom(task_instance):
    z=f"valeur du xcom {task_instance.xcom_pull(key='my_xcom_value',task_ids=['python_task'])}"
    print(z)
    task_instance.xcom_push(
        key="my_xcom_value",
        value=z
    )

with DAG(
    dag_id='simple_xcom_dag',
    schedule_interval=None,
    tags=['tutorial', 'datascientest'],
    start_date=days_ago(0)
) as dag:
    my_task = PythonOperator(
        task_id='python_task',
        python_callable=function_with_return
    )

    my_task2 = PythonOperator(
        task_id='read_xcom',
        python_callable=read_data_from_xcom
    )
    my_task >> my_task2
