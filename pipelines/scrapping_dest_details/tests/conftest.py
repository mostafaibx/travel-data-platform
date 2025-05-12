"""
Pytest configuration and fixtures for scraping destination details tests.
"""

import datetime
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from bs4 import BeautifulSoup

# Add the test directory to the sys.path to make imports work
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))


@pytest.fixture(scope="session", autouse=True)
def mock_config():
    """Mock the config module for all tests"""
    from . import config_test
    
    # Create patches for imported config values
    patches = [
        patch("pipelines.scrapping_dest_details.config.PROJECT_ID", config_test.PROJECT_ID),
        patch("pipelines.scrapping_dest_details.config.DATASET_ID", config_test.DATASET_ID),
        patch("pipelines.scrapping_dest_details.config.TABLE_ID", config_test.TABLE_ID),
        patch("pipelines.scrapping_dest_details.config.BQ_TABLE_PATH", config_test.BQ_TABLE_PATH),
        patch("pipelines.scrapping_dest_details.config.WIKIPEDIA_BASE_URL", config_test.WIKIPEDIA_BASE_URL),
        patch("pipelines.scrapping_dest_details.config.TRAVEL_DESTINATIONS", config_test.TRAVEL_DESTINATIONS),
        patch("pipelines.scrapping_dest_details.config.GCS_BUCKET_NAME", config_test.GCS_BUCKET_NAME),
        patch("pipelines.scrapping_dest_details.config.GCS_WIKI_RAW_PREFIX", config_test.GCS_WIKI_RAW_PREFIX),
    ]
    
    # Start all patches
    for p in patches:
        p.start()
    
    yield
    
    # Stop all patches after tests are done
    for p in patches:
        p.stop()


@pytest.fixture
def mock_raw_data():
    """Sample raw Wikipedia data for testing"""
    return {
        "title": "Paris",
        "extract": "Paris is the capital of France.",
        "url": "https://en.wikipedia.org/wiki/Paris",
        "status_code": 200,
        "headers": {"Content-Type": "text/html; charset=UTF-8"},
        "content": "<html><body><div id='content'>Test content</div></body></html>",
        "encoding": "UTF-8",
    }


@pytest.fixture
def mock_soup():
    """Sample BeautifulSoup object for testing"""
    html = """
    <html>
    <head><title>Paris - Wikipedia</title></head>
    <body>
        <div id="mw-content-text">
            <div>
                <p>Paris is the capital and most populous city of France.</p>
                <div class="geo">48.8566; 2.3522</div>
                <table class="infobox">
                    <tr>
                        <th>Country</th>
                        <td>France</td>
                    </tr>
                    <tr>
                        <th>Population</th>
                        <td>2,148,271 (2020)</td>
                    </tr>
                    <tr>
                        <th>Time zone</th>
                        <td>UTC+1 (CET)</td>
                    </tr>
                    <tr>
                        <th>Official language</th>
                        <td>French</td>
                    </tr>
                </table>
                <div class="image">
                    <img src="//upload.wikimedia.org/wikipedia/commons/4/4b/La_Tour_Eiffel_vue_de_la_Tour_Saint-Jacques.jpg"/>
                </div>
                <h2><span id="Climate">Climate</span></h2>
                <p>Paris has a typical Western European oceanic climate.</p>
                <h2><span id="Landmarks">Landmarks</span></h2>
                <ul>
                    <li><a href="/wiki/Eiffel_Tower">Eiffel Tower</a> - iconic tower completed in 1889</li>
                    <li><a href="/wiki/Louvre">Louvre Museum</a> - world's most visited museum</li>
                </ul>
            </div>
        </div>
    </body>
    </html>
    """
    return BeautifulSoup(html, "html.parser")


@pytest.fixture
def sample_destination_data():
    """Sample processed destination data for testing"""
    return [
        {
            "destination_name": "Paris",
            "country": "France",
            "continent": "Europe",
            "description": "Paris is the capital of France.",
            "climate": "Temperate",
            "main_attractions": ["Eiffel Tower", "Louvre Museum", "Notre-Dame"],
            "ingestion_timestamp": "2023-05-15T12:00:00",
        },
        {
            "destination_name": "Rome",
            "country": "Italy",
            "continent": "Europe",
            "description": "Rome is the capital of Italy.",
            "climate": "Mediterranean",
            "main_attractions": ["Colosseum", "Vatican", "Trevi Fountain"],
            "ingestion_timestamp": "2023-05-15T12:00:00",
        },
    ]


@pytest.fixture
def sample_processed_df(sample_destination_data):
    """Sample pandas DataFrame for testing BigQuery loading"""
    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(sample_destination_data)
    
    # Ensure the DataFrame has the expected columns for testing
    if "main_attractions" in df.columns:
        # Convert lists to strings for testing
        df["main_attractions"] = df["main_attractions"].apply(json.dumps)
    
    return df


@pytest.fixture
def mock_bigquery_client():
    """Mock BigQuery client for testing"""
    client = MagicMock()
    
    # Mock dataset methods
    mock_dataset = MagicMock()
    client.dataset.return_value = mock_dataset
    
    # Mock table methods
    mock_table = MagicMock()
    mock_dataset.table.return_value = mock_table
    
    # Mock get_dataset and get_table methods
    client.get_dataset.return_value = mock_dataset
    client.get_table.return_value = mock_table
    
    return client


@pytest.fixture
def mock_storage_client():
    """Mock Storage client for testing"""
    client = MagicMock()
    
    # Mock bucket methods
    mock_bucket = MagicMock()
    client.bucket.return_value = mock_bucket
    
    # Mock blob methods
    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    
    return client
