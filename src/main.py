from extract import extract_all_locations
from transform import transform_all_data
from load import load_all_data

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.config import API_BASE_URL, LOCATIONS, DB_CONFIG
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_pipeline():
    """Execute complete ETL pipeline."""
    logger.info("Starting weather pipeline")

    logger.info("Phase 1: Extract")
    raw_data = extract_all_locations(API_BASE_URL, LOCATIONS)

    if not raw_data:
        logger.error("No data extracted. Aborting pipeline.")
        return False

    logger.info("Phase 2: Transform")
    transformed_data = transform_all_data(raw_data)

    logger.info("Phase 3: Load")
    results = load_all_data(transformed_data, DB_CONFIG)

    logger.info(f"Pipeline complete. Results: {results}")
    return True


if __name__ == "__main__":
    success = run_pipeline()
    exit(0 if success else 1)
