.PHONY: help start stop restart logs clean setup-dev test

help:
	@echo "🏠 Lakehouse ChatBot - Available Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make setup          - Initial setup (create dirs, copy env files)"
	@echo "  make setup-dev      - Setup development environment"
	@echo ""
	@echo "Services:"
	@echo "  make start          - Start all services"
	@echo "  make start-infra    - Start infrastructure services only"
	@echo "  make start-processing - Start processing services"
	@echo "  make start-python   - Start all Python services (FastAPI, Streamlit, Airflow)"
	@echo "  make start-api      - Start FastAPI"
	@echo "  make build-ui       - Build Streamlit container"
	@echo "  make start-ui       - Start Streamlit ChatBot"
	@echo "  make start-chatbot  - Start both FastAPI and Streamlit"
	@echo "  make stop           - Stop all services"
	@echo "  make restart        - Restart all services"
	@echo ""
	@echo "Monitoring:"
	@echo "  make logs           - Show logs for all services"
	@echo "  make logs-python    - Show Python services logs (FastAPI, Streamlit, Airflow)"
	@echo "  make logs-airflow   - Show Airflow logs"
	@echo "  make logs-spark     - Show Spark logs"
	@echo "  make logs-trino     - Show Trino logs"
	@echo "  make logs-streamlit - Show Streamlit logs"
	@echo "  make logs-chatbot   - Show ChatBot (FastAPI + Streamlit) logs"
	@echo "  make logs-api       - Show FastAPI logs"
	@echo "  make logs-fastapi   - Show FastAPI logs (alias for logs-api)"
	@echo "  make ps             - Show running services"
	@echo ""
	@echo "Data:"
	@echo "  make init-data      - Initialize sample data"
	@echo "  make load-ecommerce - Load Olist e-commerce data into PostgreSQL"
	@echo "  make db-schema      - Create database schema only"
	@echo "  make db-query       - Run sample queries against database"
	@echo "  make create-buckets - Create MinIO buckets"
	@echo ""
	@echo "Development:"
	@echo "  make test           - Run tests"
	@echo "  make lint           - Run linters"
	@echo "  make format         - Format code"
	@echo ""
	@echo "Maintenance:"
	@echo "  make clean          - Clean up containers and volumes"
	@echo "  make clean-all      - Clean everything including data"

# Setup
setup:
	@echo "🔧 Setting up project..."
	@mkdir -p airflow/dags airflow/logs airflow/plugins
	@mkdir -p data/raw data/processed
	@mkdir -p notebooks
	@cp -n fastapi/.env.example fastapi/.env || true
	@cp -n streamlit/.env.example streamlit/.env || true
	@echo "✅ Setup complete!"

setup-dev:
	@echo "🔧 Setting up development environment..."
	@cd fastapi && pip install -r requirements.txt
	@cd streamlit && pip install -r requirements.txt
	@echo "✅ Development environment ready!"

# Services
start:
	@echo "🚀 Starting all services..."
	docker-compose up -d
	@echo "✅ All services started!"
	@echo ""
	@make urls

start-infra:
	@echo "🚀 Starting infrastructure services..."
	docker-compose up -d postgres kafka minio metastore-db hive-metastore redis
	@echo "✅ Infrastructure services started!"

start-processing:
	@echo "🚀 Starting processing services..."
	docker-compose up -d spark-master spark-worker trino-coordinator trino-worker
	@echo "✅ Processing services started!"

start-airflow:
	@echo "🚀 Starting Airflow..."
	docker-compose up -d airflow-db airflow-webserver airflow-scheduler airflow-worker airflow-triggerer
	@echo "✅ Airflow started!"

start-api:
	@echo "🚀 Starting FastAPI..."
	cd fastapi && uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

build-ui:
	@echo "🔨 Building Streamlit..."
	docker-compose build streamlit

start-ui:
	@echo "🚀 Starting Streamlit ChatBot..."
	docker-compose up -d streamlit

start-python:
	@echo "🐍 Starting all Python services (FastAPI, Streamlit, Airflow)..."
	docker-compose up -d chatbot-api streamlit airflow-init airflow-dag-processor airflow-scheduler airflow-apiserver

start-chatbot:
	@echo "🤖 Starting ChatBot (FastAPI + Streamlit)..."
	docker-compose up -d chatbot-api streamlit

stop:
	@echo "⏸️  Stopping all services..."
	docker-compose down
	@echo "✅ All services stopped!"

restart:
	@make stop
	@make start

# Monitoring
logs:
	docker-compose logs -f

logs-airflow:
	docker-compose logs -f airflow-webserver airflow-scheduler airflow-worker

logs-spark:
	docker-compose logs -f spark-master spark-worker

logs-trino:
	docker-compose logs -f trino-coordinator

logs-streamlit:
	docker-compose logs -f streamlit

logs-python:
	docker-compose logs -f chatbot-api streamlit airflow-init airflow-dag-processor airflow-scheduler airflow-apiserver

logs-chatbot:
	docker-compose logs -f chatbot-api streamlit

logs-api:
	docker-compose logs -f chatbot-api

logs-fastapi:
	docker-compose logs -f chatbot-api

ps:
	docker-compose ps

# Data
init-data:
	@echo "📊 Initializing sample data..."
	docker-compose exec postgres psql -U postgres -d ecommerce -f /docker-entrypoint-initdb.d/01-ecommerce-schema.sql
	@echo "✅ Sample data initialized!"

load-ecommerce:
	@echo "📦 Loading Olist E-commerce data into PostgreSQL..."
	@echo "This may take several minutes (loading ~1.5M records)..."
	@bash scripts/setup_postgres_demo.sh
	@echo "✅ E-commerce data loaded successfully!"
	@echo ""
	@echo "💡 Try some queries: make db-query"

db-schema:
	@echo "🗄️  Creating database schema..."
	@export PGPASSWORD="${POSTGRES_PASSWORD:-postgres}"; \
	psql -h ${POSTGRES_HOST:-localhost} -p ${POSTGRES_PORT:-5432} \
		-U ${POSTGRES_USER:-postgres} -d ${POSTGRES_DB:-postgres} \
		-f sql/init/01-ecommerce-schema.sql
	@echo "✅ Database schema created!"

db-query:
	@echo "📊 Running sample queries..."
	@export PGPASSWORD="${POSTGRES_PASSWORD:-postgres}"; \
	psql -h ${POSTGRES_HOST:-localhost} -p ${POSTGRES_PORT:-5432} \
		-U ${POSTGRES_USER:-postgres} -d ${POSTGRES_DB:-postgres} \
		-f sql/queries/sample_queries.sql

db-shell:
	@echo "🐘 Connecting to PostgreSQL..."
	@export PGPASSWORD="${POSTGRES_PASSWORD:-postgres}"; \
	psql -h ${POSTGRES_HOST:-localhost} -p ${POSTGRES_PORT:-5432} \
		-U ${POSTGRES_USER:-postgres} -d ${POSTGRES_DB:-postgres}

create-buckets:
	@echo "🪣 Creating MinIO buckets..."
	docker-compose up -d minio-setup
	@echo "✅ Buckets created!"

# Development
test:
	@echo "🧪 Running tests..."
	cd fastapi && pytest tests/
	@echo "✅ Tests complete!"

lint:
	@echo "🔍 Running linters..."
	cd fastapi && pylint app/
	cd streamlit && pylint *.py
	@echo "✅ Linting complete!"

format:
	@echo "✨ Formatting code..."
	black fastapi/ streamlit/ spark-jobs/
	isort fastapi/ streamlit/ spark-jobs/
	@echo "✅ Formatting complete!"

# Maintenance
clean:
	@echo "🧹 Cleaning up..."
	docker-compose down -v
	@echo "✅ Cleanup complete!"

clean-all:
	@echo "🧹 Cleaning everything..."
	docker-compose down -v --remove-orphans
	rm -rf airflow/logs/*
	rm -rf data/raw/*
	rm -rf data/processed/*
	@echo "✅ Deep cleanup complete!"

# URLs
urls:
	@echo "📌 Access URLs:"
	@echo "  Streamlit ChatBot:  http://localhost:8501"
	@echo "  FastAPI Docs:       http://localhost:5000/docs"
	@echo "  Metabase:           http://localhost:3000"
	@echo "  Airflow:            http://localhost:8081 (admin/admin)"
	@echo "  Spark Master:       http://localhost:8080"
	@echo "  Trino:              http://localhost:8083"
	@echo "  MinIO Console:      http://localhost:9001 (admin/password123)"
	@echo "  Kafka UI:           http://localhost:8085"

# Health checks
health:
	@echo "🏥 Checking service health..."
	@curl -s http://localhost:8000/health || echo "❌ FastAPI not responding"
	@curl -s http://localhost:8086/health || echo "❌ Airflow not responding"
	@curl -s http://localhost:3000/api/health || echo "❌ Metabase not responding"
	@echo "✅ Health check complete!"
