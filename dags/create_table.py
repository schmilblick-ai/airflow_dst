	
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow import settings
from airflow.models.connection import Connection

def create_postgres_conn(**kwargs):
    session = settings.Session()
    print("Session created")
    connections = session.query(Connection)
    print("Connections listed")
    if not kwargs['conn_id'] in [connection.conn_id for connection in connections]:
        conn = Connection(
            conn_id=kwargs['conn_id'],
            conn_type='postgres',
            host='postgres',
            login='airflow',
            password='airflow',
            schema='airflow'
        )    
        session.add(conn)
        session.commit()
        print("Connection Created")
    else:
        print("Connection already exists")
    session.close()
with DAG(
    dag_id='create_conn',
    tags=['postgres', 'datascientest'],
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    },
    catchup=False
) as dag:
    create_conn = PythonOperator(
        task_id="create_postgres_conn",
        python_callable=create_postgres_conn,
        op_kwargs={'conn_id': 'postgres'}
    )
