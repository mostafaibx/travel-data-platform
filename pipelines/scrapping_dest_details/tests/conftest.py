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
        patch(
            "pipelines.scrapping_dest_details.config.PROJECT_ID", config_test.PROJECT_ID
        ),
        patch(
            "pipelines.scrapping_dest_details.config.DATASET_ID", config_test.DATASET_ID
        ),
        patch("pipelines.scrapping_dest_details.config.TABLE_ID", config_test.TABLE_ID),
        patch(
            "pipelines.scrapping_dest_details.config.BQ_TABLE_PATH",
            config_test.BQ_TABLE_PATH,
        ),
        patch(
            "pipelines.scrapping_dest_details.config.WIKIPEDIA_BASE_URL",
            config_test.WIKIPEDIA_BASE_URL,
        ),
        patch(
            "pipelines.scrapping_dest_details.config.TRAVEL_DESTINATIONS",
            config_test.TRAVEL_DESTINATIONS,
        ),
        patch(
            "pipelines.scrapping_dest_details.config.GCS_BUCKET_NAME",
            config_test.GCS_BUCKET_NAME,
        ),
        patch(
            "pipelines.scrapping_dest_details.config.GCS_WIKI_RAW_PREFIX",
            config_test.GCS_WIKI_RAW_PREFIX,
        ),
    ]

    # Start all patches
    for p in patches:
        p.start()

    yield

    # Stop all patches after tests are done
    for p in patches:
        p.stop()


@pytest.fixture
def mock_soup():
    """
    Create a mock BeautifulSoup object with sample Wikipedia HTML content
    """
    html = """
    <html>
    <head><title>Paris - Wikipedia</title></head>
    <body>
        <div id="mw-content-text">
            <div>
                <p class="mw-empty-elt"></p>
                <p>Paris is the capital and most populous city of France. With an estimated population of 2,148,271 residents,
                Paris is the fourth-most populated city in the European Union.</p>
                <div class="infobox">
                    <table>
                        <tr><th>Country</th><td>France</td></tr>
                        <tr><th>Region</th><td>Île-de-France</td></tr>
                        <tr><th>Time zone</th><td>UTC+1 (CET)</td></tr>
                        <tr><th scope="row">Population</th></tr>
                        <tr><th>• City</th><td>2,148,271 (2020)</td></tr>
                        <tr><th>Official language</th><td>French</td></tr>
                    </table>
                </div>
                <div class="geo">48.8566; 2.3522</div>
            </div>
        </div>
        <span id="Landmarks" class="mw-headline">Landmarks</span>
        <ul>
            <li><a href="/wiki/Eiffel_Tower">Eiffel Tower</a> - iconic tower completed in 1889</li>
            <li><a href="/wiki/Louvre">Louvre Museum</a> - world's most visited museum</li>
        </ul>
        <span id="Climate" class="mw-headline">Climate</span>
        <p>Paris has a typical Western European oceanic climate which is affected by the North Atlantic Current.</p>
        <div class="thumb">
            <img src="https://upload.wikimedia.org/wikipedia/commons/4/4b/La_Tour_Eiffel_vue_de_la_Tour_Saint-Jacques.jpg"
                 alt="Paris skyline" />
        </div>
    </body>
    </html>
    """
    return BeautifulSoup(html, "html.parser")


@pytest.fixture
def mock_raw_data():
    """Sample raw Wikipedia data for testing"""
    return {
        "title": "Paris",
        "extract": "Paris is the capital of France.",
        "url": "https://en.wikipedia.org/wiki/Paris",
    }


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
def sample_processed_df():
    """
    Sample DataFrame prepared for BigQuery
    """
    data = [
        {
            "destination_name": "Paris",
            "description": "Paris is the capital and most populous city of France.",
            "country": "France",
            "latitude": 48.8566,
            "longitude": 2.3522,
            "population_count": 2148271,
            "population_year": 2020,
            "timezone": "UTC+1 (CET)",
            "languages": json.dumps(["French"]),
            "climate": "Paris has a typical Western European oceanic climate.",
            "image_url": "https://upload.wikimedia.org/wikipedia/commons/4/4b/La_Tour_Eiffel_vue_de_la_Tour_Saint-Jacques.jpg",
            "sections": json.dumps(["History", "Geography", "Climate", "Landmarks"]),
            "area_km2": None,
            "region": "Île-de-France",
            "attractions_count": 2,
            "attractions": json.dumps(
                [
                    {
                        "name": "Eiffel Tower",
                        "description": "iconic tower completed in 1889",
                        "type": "Landmark",
                    },
                    {
                        "name": "Louvre Museum",
                        "description": "world's most visited museum",
                        "type": "Museum",
                    },
                ]
            ),
            "ingestion_timestamp": datetime.datetime(2023, 5, 15, 12, 0, 0),
        }
    ]
    return pd.DataFrame(data)


@pytest.fixture
def mock_requests_session():
    """
    Mock requests.Session for testing
    """
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "<html>...</html>"
    mock_response.headers = {"Content-Type": "text/html"}
    mock_response.encoding = "UTF-8"
    mock_session.get.return_value = mock_response
    return mock_session


@pytest.fixture
def mock_bq_loader():
    """
    Mock BigQueryLoader for testing
    """
    mock_loader = MagicMock()
    mock_loader.upload_with_merge.return_value = True
    return mock_loader
