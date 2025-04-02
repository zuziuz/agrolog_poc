"""
Helper utilities for the trucking application.
"""
import os
import logging
import tempfile
import pandas as pd
from typing import Dict, List, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ensure_directories_exist():
    """Ensure that any required directories exist."""
    # Create directories for temp file storage if needed
    os.makedirs("tmp", exist_ok=True)


def load_config(secrets) -> Dict[str, Any]:
    """
    Load and validate configuration from Streamlit secrets.

    Args:
        secrets: Streamlit secrets object

    Returns:
        Dict with validated configuration
    """
    try:
        config = secrets["config"]

        # Validate required configuration fields
        required_fields = [
            "google_address_validator_api_key",
            "project_id",
            "api_base_url",
            "loctracker_api_username",
            "loctracker_api_password",
            "bigquery_service_account_json"
        ]

        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required configuration field: {field}")


        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        raise


def check_csv_schema(df: pd.DataFrame) -> List[str]:
    """
    Check if a CSV has the required columns for task processing.

    Args:
        df: DataFrame to check

    Returns:
        List of missing required columns
    """
    required_columns = ['localId', 'deviceNumber', 'locationAddress']
    return [col for col in required_columns if col not in df.columns]


def clean_address(address: str) -> str:
    """
    Clean an address string by removing newlines and extra spaces.

    Args:
        address: Address string to clean

    Returns:
        Cleaned address as a single line
    """
    if not address:
        return ""

    # Replace newlines with spaces and remove extra spaces
    return " ".join(address.replace("\n", " ").split())


def create_temp_dir() -> str:
    """
    Create a temporary directory for file storage.

    Returns:
        Path to temporary directory
    """
    temp_dir = tempfile.mkdtemp(prefix="trucking_app_")
    return temp_dir