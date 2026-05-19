from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor
from airflow.operators.docker_operator import DockerOperator
from docker.types import Mount
from plugins.build_init_order import build_init_order

#  Le type Mount nous permet d'ajouter un volume à nos conteneurs.
#	Ajoutez la définition du DAG suivante :
with DAG(
    dag_id='load_order',
    tags=['order', 'docker', 'postgres', 'datascientest'],
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    },
    #schedule_interval='0 17 * * *',
    catchup=False
) as dag:
 
    init_order = build_init_order(dag)

    #  Notre DAG s'exécutera tous les jours à 17h.
    #  À la suite de la déclaration du DAG, créez la première tâche qui "réussit" si le fichier airflow/data/to_ingest/bronze/orders.json existe.
    orders_sensor = FileSensor(
        task_id='orders_sensor',
        filepath='data/to_ingest/bronze/orders.json',
        poke_interval=20,
        timeout=120,
        mode='poke'
    )
 
    #  Ajoutez la tâche des DockerOperator :
	#  python_transform :
    python_transform = DockerOperator(
        task_id='python_transform',
        image='python_transform:latest',
        auto_remove=True,
        command='python3 main.py',
        mounts=[
            Mount(source='/home/ubuntu/airflow/data/to_ingest', target='/app/data/to_ingest', type='bind')
        ]
    )

    #  Nous avons ajouté l'option auto_remove qui supprime le conteneur à la fin de son exécution et l'option mounts qui crée un volume.
	#  python_load :
    python_load = DockerOperator(
        task_id='python_load',
        image='python_load:latest',
        auto_remove=True,
        environment={
            'HOST': 'postgres',
            'DATABASE': 'airflow',
            'USER': 'airflow',
            'PASSWORD': 'airflow'
        },
        command='python3 main.py',
        network_mode='airflow_default',
        mounts=[
            Mount(source='/home/ubuntu/airflow/data/to_ingest', target='/app/data/to_ingest', type='bind')
        ]
    )

#  Nous spécifions les variables d'environnement via l'option environment et le nom du réseau Docker via l'option network_mode.
#	Définissez les dépendances de vos tâches.

init_order >> orders_sensor >> python_transform >> python_load

# Génial, nous avons nos deux DAGs pour notre ETL bien complex. 
# Nous les lancons ensemble en utilisant un TaskGroup afin d'englober les tâches de notre pipeline de setup
