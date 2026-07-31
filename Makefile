# Real-Time Analytics Platform - Makefile
# ==========================================

.PHONY: help build up down logs test clean setup kafka-topics spark-submit dbt-run report

help: ## Show this help message
	@echo "Real-Time Analytics Platform"
	@echo "============================"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ------------------------------------------
# Docker Operations
# ------------------------------------------
build: ## Build all Docker images
	docker-compose build

up: ## Start all services in detached mode
	docker-compose up -d

up-logs: ## Start all services with logs attached
	docker-compose up

down: ## Stop and remove all containers
	docker-compose down

down-volumes: ## Stop containers and remove volumes
	docker-compose down -v

logs: ## Tail logs from all services
	docker-compose logs -f

logs-airflow: ## Tail Airflow logs
	docker-compose logs -f airflow-webserver airflow-scheduler airflow-worker

logs-spark: ## Tail Spark logs
	docker-compose logs -f spark-master spark-worker

logs-kafka: ## Tail Kafka logs
	docker-compose logs -f kafka

# ------------------------------------------
# Setup & Initialization
# ------------------------------------------
setup: ## Initial setup - copy env file and create topics
	@if [ ! -f .env ]; then cp .env.example .env; echo ".env created from .env.example"; fi
	@chmod +x kafka/topics.sh
	@echo "Setup complete. Edit .env with your credentials."

init-db: ## Initialize Snowflake database objects
	@echo "Run snowflake/setup.sql in your Snowflake console or via SnowSQL"

kafka-topics: ## Create Kafka topics
	@bash kafka/topics.sh

# ------------------------------------------
# Pipeline Execution
# ------------------------------------------
spark-batch: ## Submit Spark batch job
	docker exec -it realtime-analytics-platform-spark-master-1 		spark-submit --master spark://spark-master:7077 		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 		/opt/spark/work-dir/batch_processing.R

spark-streaming: ## Submit Spark streaming job
	docker exec -it realtime-analytics-platform-spark-master-1 		spark-submit --master spark://spark-master:7077 		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 		/opt/spark/work-dir/streaming_processing.R

dbt-run: ## Run dbt models
	docker-compose run --rm airflow-worker 		bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir ."

dbt-test: ## Run dbt tests
	docker-compose run --rm airflow-worker 		bash -c "cd /opt/airflow/dbt && dbt test --profiles-dir ."

dbt-docs: ## Generate and serve dbt docs
	docker-compose run --rm -p 8085:8085 airflow-worker 		bash -c "cd /opt/airflow/dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir . --port 8085"

# ------------------------------------------
# R Analytics
# ------------------------------------------
shiny: ## Start Shiny dashboard (if not using Docker)
	cd r_analytics && R -e "shiny::runApp('app.R', host='0.0.0.0', port=3838)"

report: ## Generate daily R Markdown report
	cd r_analytics && Rscript -e "rmarkdown::render('reports/daily_report.Rmd', output_file='../output/daily_report.html')"

r-shell: ## Open R shell in analytics container
	docker exec -it realtime-analytics-platform-r-analytics-1 R

# ------------------------------------------
# Testing & Quality
# ------------------------------------------
test: ## Run R pipeline tests
	cd tests && Rscript test_pipeline.R

lint-r: ## Lint R code
	cd r_analytics && Rscript -e "lintr::lint_dir()"

# ------------------------------------------
# Maintenance
# ------------------------------------------
clean: ## Clean up generated files and Docker artifacts
	@rm -rf airflow/logs/*
	@rm -rf output/*
	@docker system prune -f
	@echo "Cleanup complete."

restart: down up ## Restart all services

status: ## Show running containers
	docker-compose ps
