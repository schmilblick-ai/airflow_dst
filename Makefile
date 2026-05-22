# var AIRFLOW_PROJ_DIR utile en cas de docker 
export AIRFLOW_PROJ_DIR := $(shell pwd)

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


exam_creadb:
	docker-compose -f docker-compose-exam.yaml up airflow-init

exam_start:
	docker-compose -f docker-compose-exam.yaml up -d

exam_down:
	docker-compose -f docker-compose.yaml down 

exam_ps:
	docker-compose -f docker-compose-exam.yaml ps

clean-logs: ## Supprime les logs Airflow
	find logs/ -name "*.log" -delete

clean-cache: ## Nettoie le cache de build Docker
	docker builder prune -f

clean-venv: ## Supprime le venv
	rm -rf .venv

restore-venv: ## restore le venv
	uv sync
