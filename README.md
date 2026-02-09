# Weather Pipeline

ETL pipeline that extracts weather data from the [Open-Meteo API](https://open-meteo.com/), transforms the raw JSON into structured data, and loads it into PostgreSQL. Orchestrated with Apache Airflow.

Built following the [Building Your First Pipeline: From Concept to Execution](https://dev.to/qvfagundes/building-your-first-pipeline-from-concept-to-execution-42nd) article by Vinicius Fagundes.

## Project Structure

```
weather-pipeline/
├── config/          # Database and API configuration
├── dags/            # Airflow DAG definitions
├── db-init/         # PostgreSQL initialization scripts
├── src/
│   ├── extract.py   # API data extraction
│   ├── transform.py # Data cleaning and structuring
│   ├── load.py      # PostgreSQL loading with upsert
│   └── main.py      # Pipeline entry point
└── tests/           # Unit tests
```

## Setup

### Prerequisites

- Docker and Docker Compose

### Start all services

```bash
docker compose up -d
```

This starts PostgreSQL, the Airflow webserver (port 8080), and the Airflow scheduler. On first run it initializes both the `weather_db` and `airflow_db` databases and creates an admin user.

### Airflow UI

Open http://localhost:8080 and log in with `admin` / `admin`. Trigger the `weather_pipeline` DAG to run the ETL.

### Run the pipeline manually (without Airflow)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/main.py
```

### Run tests

```bash
pytest tests/
```

### Verify data

```bash
docker compose exec db psql -U postgres -d weather_db -c "SELECT COUNT(*) FROM hourly_weather;"
```

## Technologies

- **Python** - primary language
- **Apache Airflow** - orchestration and scheduling
- **PostgreSQL** - data storage
- **Docker Compose** - local environment (Airflow + PostgreSQL)
- **pandas** - data transformation
- **requests** - API calls
- **psycopg2** - PostgreSQL adapter
