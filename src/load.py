import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from typing import Dict
import logging

logger = logging.getLogger(__name__)


def get_connection(db_config: Dict):
    """Create database connection."""
    return psycopg2.connect(**db_config)


def create_tables(db_config: Dict) -> None:
    """
    Create tables if they don't exist.

    Args:
        db_config: Database configuration dictionary
    """
    create_current_table = """
    CREATE TABLE IF NOT EXISTS current_weather (
        id SERIAL PRIMARY KEY,
        location_name VARCHAR(100),
        latitude DECIMAL(9,6),
        longitude DECIMAL(9,6),
        temperature_celsius DECIMAL(5,2),
        wind_speed_kmh DECIMAL(6,2),
        wind_direction_degrees INTEGER,
        weather_code INTEGER,
        observation_time TIMESTAMP,
        extracted_at TIMESTAMP,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(location_name, observation_time)
    );
    """

    create_hourly_table = """
    CREATE TABLE IF NOT EXISTS hourly_weather (
        id SERIAL PRIMARY KEY,
        location_name VARCHAR(100),
        forecast_time TIMESTAMP,
        temperature_celsius DECIMAL(5,2),
        relative_humidity_percent INTEGER,
        wind_speed_kmh DECIMAL(6,2),
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(location_name, forecast_time)
    );
    """

    conn = get_connection(db_config)
    try:
        with conn.cursor() as cur:
            cur.execute(create_current_table)
            cur.execute(create_hourly_table)
        conn.commit()
        logger.info("Tables created successfully")
    finally:
        conn.close()


def load_dataframe(df: pd.DataFrame, table_name: str, db_config: Dict) -> int:
    """
    Load DataFrame into PostgreSQL table using upsert.

    Args:
        df: DataFrame to load
        table_name: Target table name
        db_config: Database configuration

    Returns:
        Number of rows loaded
    """
    if df.empty:
        logger.warning(f"No data to load into {table_name}")
        return 0

    conn = get_connection(db_config)
    try:
        columns = df.columns.tolist()
        values = [tuple(row) for row in df.values]

        insert_query = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES %s
        ON CONFLICT DO NOTHING
        """

        with conn.cursor() as cur:
            execute_values(cur, insert_query, values, page_size=len(values))
            rows_affected = cur.rowcount

        conn.commit()
        logger.info(f"Loaded {rows_affected} rows into {table_name}")
        return rows_affected

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to load data into {table_name}: {e}")
        raise
    finally:
        conn.close()


def load_all_data(data: Dict[str, pd.DataFrame], db_config: Dict) -> Dict[str, int]:
    """
    Load all transformed data into database.

    Args:
        data: Dictionary with DataFrames
        db_config: Database configuration

    Returns:
        Dictionary with row counts per table
    """
    create_tables(db_config)

    results = {}
    results["current_weather"] = load_dataframe(
        data["current"], "current_weather", db_config
    )
    results["hourly_weather"] = load_dataframe(
        data["hourly"], "hourly_weather", db_config
    )

    return results
