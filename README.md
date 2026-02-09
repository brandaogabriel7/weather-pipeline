# Weather Pipeline

ETL pipeline that extracts weather data from the [Open-Meteo API](https://open-meteo.com/), transforms the raw JSON into structured data, and loads it into PostgreSQL. Orchestrated with Apache Airflow.

Built following the [Building Your First Pipeline: From Concept to Execution](https://dev.to/qvfagundes/building-your-first-pipeline-from-concept-to-execution-42nd) article by Vinicius Fagundes.

## Project Structure

```
weather-pipeline/
├── config/          # Database and API configuration
├── dags/            # Airflow DAG definitions
├── src/
│   ├── extract.py   # API data extraction
│   ├── transform.py # Data cleaning and structuring
│   ├── load.py      # PostgreSQL loading with upsert
│   └── main.py      # Pipeline entry point
└── tests/           # Unit tests
```

## Setup

### Prerequisites

- Python 3.12+
- Docker (for PostgreSQL)

### Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Start the database

```bash
docker compose up -d
```

### Run the pipeline

```bash
python src/main.py
```

### Run tests

```bash
pytest tests/
```

## Technologies

- **Python** - primary language
- **Apache Airflow** - orchestration and scheduling
- **PostgreSQL** - data storage
- **pandas** - data transformation
- **requests** - API calls
- **psycopg2** - PostgreSQL adapter
