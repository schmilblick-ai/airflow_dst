# Importez les librairies nécessaires au fonctionnement du DAG.

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow import settings
from airflow.models.connection import Connection
from airflow.providers.postgres.operators.postgres import PostgresOperator
	
# Définissez la configuration des Connections.
postgres_conn_conf = {
    'conn_id': 'postgres',
    'conn_type': 'postgres',
    'host': 'postgres',
    'login': 'airflow',
    'password': 'airflow',
    'schema': 'airflow'
}
fs_default_conn_conf = {
    'conn_id': 'fs_default',
    'conn_type': 'File',
    'host': '/opt/airflow/',
    'login': None,
    'password': None,
    'schema': None
}

#  Nous avons délibéremment spécifié les mêmes attributs afin d'optimiser l'écriture de notre code. Nous allons voir comment dans un instant.
# Créez une liste nommée conn_keys afin de spécifier les clés de nos dictionnaires de configuration.
	
conn_keys = ['conn_id', 'conn_type', 'host', 'login', 'password', 'schema']

# Créez la fonction create_conn qui crée une Connection à partir d'un dictionnaire de configuration si celle-ci n'existe pas déjà.
	
def create_conn(**kwargs):
    session = settings.Session()
    print("Session created")
    connections = session.query(Connection)
    print("Connections listed")
    if not kwargs['conn_id'] in [connection.conn_id for connection in connections]:
        conn_params = { key: kwargs[key] for key in conn_keys }
        conn = Connection(**conn_params)
        session.add(conn)
        session.commit()
        print("Connection Created")
    else:
        print("Connection already exists")
    session.close()

"""
Voici quelques explications du code ci-dessus :
	• Nous créons un objet de la classe Session afin de manipuler nos Connections.
	• Nous récupérons la liste de nos Connections.
	• Nous cherchons si la connexion que nous souhaitons créer existe ou non.
	• Si elle n'existe pas :
		○ Nous redéfinissons le dictionnaire de configuration de notre Connection car nous retrouvons des clés générées automatiquement par Airflow dans le dictionnaire kwargs.
		○ Nous créons un objet de la classe Connection à l'aide du dictionnaire précédemment créé.
		○ Nous ajoutons la Connection et nous lançons un commit afin de mettre à jour la base de données d'Airflow.
	• Nous fermons la session.
"""

# Créez le DAG ainsi que les 2 tâches afin de créer nos Connections.
with DAG(
    dag_id='init_order',
    tags=['order', 'datascientest'],
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    },
    catchup=False
) as dag:
    create_postgres_conn = PythonOperator(
        task_id='create_postgres_conn',
        python_callable=create_conn,
        op_kwargs=postgres_conn_conf
    )
    create_fs_default_conn = PythonOperator(
        task_id='create_fs_default_conn',
        python_callable=create_conn,
        op_kwargs=fs_default_conn_conf
    )

    create_table_customer = PostgresOperator(
        task_id='create_table_customer',
        postgres_conn_id='postgres',
        sql='sql/create_table_customer.sql'
    )
    create_table_product = PostgresOperator(
        task_id='create_table_product',
        postgres_conn_id='postgres',
        sql='sql/create_table_product.sql'
    )
    create_table_order = PostgresOperator(
        task_id='create_table_order',
        postgres_conn_id='postgres',
        sql='sql/create_table_order.sql'
    )
    #Définissez les dépendances de vos tâches.
    create_fs_default_conn
    create_postgres_conn >> [create_table_customer, create_table_product]
    [create_table_customer, create_table_product] >> create_table_order
