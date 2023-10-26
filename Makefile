up:
	docker compose up -d
down:
	docker compose down
init:
	docker compose up airflow-init
exec:
	docker exec -it airflow-airflow-scheduler-1 sh
