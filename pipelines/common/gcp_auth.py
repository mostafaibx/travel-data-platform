"""
Google Cloud Platform authentication utilities.
This module handles authentication with GCP services across all pipelines.
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

from google.cloud import bigquery
from google.oauth2 import service_account

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("gcp_auth")

# Default paths for credentials
DEFAULT_KEYS_DIR = Path.home() / ".gcp"
DEFAULT_KEY_FILENAME = "service-account-key.json"


def get_credentials(
    key_path: Optional[str] = None, project_id: Optional[str] = None
) -> service_account.Credentials:
    """
    Get GCP credentials from various possible sources.

    Args:
        key_path: Optional explicit path to a service account key file
        project_id: Optional project ID to override the one in the key file

    Returns:
        GCP service account credentials object

    Raises:
        FileNotFoundError: If no valid credentials are found
    """
    # Priority 1: Explicitly provided path
    if key_path and os.path.exists(key_path):
        logger.info(f"Using credentials from explicitly provided path: {key_path}")
        credentials = service_account.Credentials.from_service_account_file(key_path)

    # Priority 2: Environment variable path
    elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if os.path.exists(env_path):
            logger.info(
                f"Using credentials from GOOGLE_APPLICATION_CREDENTIALS: {env_path}"
            )
            credentials = service_account.Credentials.from_service_account_file(
                env_path
            )
        else:
            raise FileNotFoundError(
                f"Credentials file specified in GOOGLE_APPLICATION_CREDENTIALS not found: {env_path}"
            )

    # Priority 3: Default location
    elif os.path.exists(DEFAULT_KEYS_DIR / DEFAULT_KEY_FILENAME):
        default_path = DEFAULT_KEYS_DIR / DEFAULT_KEY_FILENAME
        logger.info(f"Using credentials from default location: {default_path}")
        credentials = service_account.Credentials.from_service_account_file(
            default_path
        )

    # No credentials found
    else:
        raise FileNotFoundError(
            "No GCP credentials found. Please either:"
            "\n1. Set GOOGLE_APPLICATION_CREDENTIALS environment variable"
            f"\n2. Place credentials at {DEFAULT_KEYS_DIR / DEFAULT_KEY_FILENAME}"
            "\n3. Provide an explicit key_path parameter"
        )

    # We don't need to override project_id in credentials anymore
    # as we'll pass it directly to the BigQuery client
    return credentials


def get_bigquery_client(
    key_path: Optional[str] = None,
    project_id: Optional[str] = None,
    location: str = "US",
) -> bigquery.Client:
    """
    Get an authenticated BigQuery client.

    Args:
        key_path: Optional explicit path to a service account key file
        project_id: Optional project ID to override the one in the key file
        location: BigQuery location/region to use (default: "US")

    Returns:
        Authenticated BigQuery client
    """
    credentials = get_credentials(key_path, project_id)

    # Use the project ID from credentials if not explicitly provided
    if not project_id and hasattr(credentials, "project_id"):
        project_id = credentials.project_id

    client = bigquery.Client(
        credentials=credentials, project=project_id, location=location
    )

    logger.info(f"Created BigQuery client for project: {project_id}")
    return client


def save_key_to_default_location(
    key_data: Union[str, Dict[str, Any]], create_dirs: bool = True
) -> Path:
    """
    Save a service account key to the default location.

    Args:
        key_data: Either a JSON string or dictionary containing the key data
        create_dirs: Whether to create the default directory if it doesn't exist

    Returns:
        Path where the key was saved
    """
    # Create the default directory if it doesn't exist
    if create_dirs and not DEFAULT_KEYS_DIR.exists():
        DEFAULT_KEYS_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created default key directory: {DEFAULT_KEYS_DIR}")

    key_path = DEFAULT_KEYS_DIR / DEFAULT_KEY_FILENAME

    # Convert dict to JSON string if necessary
    if isinstance(key_data, dict):
        key_data = json.dumps(key_data, indent=2)

    # Write the key data to the file
    with open(key_path, "w") as f:
        f.write(key_data)

    # Set restrictive permissions on the key file
    os.chmod(key_path, 0o600)  # Only owner can read/write

    logger.info(f"Saved service account key to: {key_path}")
    return key_path


def verify_credentials(credentials: service_account.Credentials) -> bool:
    """
    Verify that the provided credentials are valid by making a simple API call.

    Args:
        credentials: The credentials to verify

    Returns:
        True if credentials are valid, False otherwise
    """
    try:
        # Create a client with the credentials
        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

        # Make a simple API call
        _ = list(client.list_datasets(max_results=1))

        logger.info(
            f"Credentials verified successfully for project: {credentials.project_id}"
        )
        return True
    except Exception as e:
        logger.error(f"Credential verification failed: {str(e)}")
        return False
