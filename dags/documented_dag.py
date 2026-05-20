from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import time, pendulum

def sleep_1_sec():
        time.sleep(1)

with DAG(
    dag_id="documented_dag",
    doc_md="""# Documented DAG
This `DAG` is documented and the next line is a quote:
> Airflow is nice
This DAG has been made:
* by DataScientest
* with documentation
* with caution
    """,
    #start_date=days_ago(0),
    #start_date=pendulum.today('UTC').add(days=0),
    tags=['tutorial', 'datascientest'],
    schedule_interval=None,
    schedule=None,
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id="sleep1",
        python_callable=sleep_1_sec,
        doc_md="""# Task1
Task that is used to sleep for 1 sec""",
    )
    task2 = PythonOperator(
        task_id="sleep2",
        python_callable=sleep_1_sec,
        doc="""Task 3
It has an ugly description.
    """
    )
    with open("/opt/airflow/dags/documented_dag.md") as f:
        docMD = f.read()

    task3 = PythonOperator(
        task_id="sleep3",
        python_callable=sleep_1_sec,
        doc_md=docMD
    )

    task1 >> task2 >> task3
