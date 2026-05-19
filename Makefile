#pour première installation ou mise à jour config et image
airflow_creadb:
	docker-compose up airflow-init

	# C'est la commande d'initialisation — à lancer une seule fois (ou après une mise à jour majeure). Elle effectue :

	# La création et migration de la base de données (tables Airflow, métadonnées)
	# La création de l'utilisateur admin par défaut
	# Les vérifications de configuration initiale

	# Elle se termine d'elle-même une fois le setup accompli.

#Démarrage quotidien en mode daemon -d
airflow_start:
	docker-compose up -d

airflow_force_recreate:
	docker compose up --force-recreate

airflow_down:
	docker-compose down 

airflow_reset:
	echo "docker-compose down -v"

airflow_ps:
	docker-compose ps

airflow_prepdocker_sock:
	sudo chmod a+rw /var/run/docker.sock
