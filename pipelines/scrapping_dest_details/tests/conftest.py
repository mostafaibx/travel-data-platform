#!/usr/bin/env python3
"""
Pytest configuration for the scrapping_dest_details tests.
"""
import json
import os
from unittest.mock import MagicMock, patch

import pytest
from bs4 import BeautifulSoup


@pytest.fixture
def wikipedia_scraper():
    """Returns a WikipediaScraper instance with no rate limiting for tests"""
    from ..fetcher import WikipediaScraper

    return WikipediaScraper(rate_limit_delay=0)  # No delay in tests


@pytest.fixture
def sample_infobox_html():
    """Sample HTML for a Wikipedia infobox"""
    return """
    <table class="infobox">
        <tbody>
            <tr><th>Country</th><td>United States</td></tr>
            <tr><th>State</th><td>New York</td></tr>
            <tr><th>Population</th></tr>
            <tr><td>• Total</td><td>8,804,190 (2020)</td></tr>
            <tr><th>Time zone</th><td>UTC−05:00 (EST)</td></tr>
        </tbody>
    </table>
    """


@pytest.fixture
def sample_coordinates_html():
    """Sample HTML for geographic coordinates"""
    return '<span class="geo">40.7128; -74.0060</span>'


@pytest.fixture
def sample_description_html():
    """Sample HTML for a description paragraph"""
    return """
    <div id="mw-content-text">
        <div class="mw-parser-output">
            <p class="mw-empty-elt"></p>
            <p><b>New York City</b> (NYC), often called simply <b>New York</b>, is the most populous city in the United States.[2]
            With an estimated 2019 population of 8,336,817 distributed over about 302.6 square miles (784 km<sup>2</sup>),
            New York City is also the most densely populated major city in the United States.[3]</p>
        </div>
    </div>
    """


@pytest.fixture
def sample_attractions_html():
    """Sample HTML for attractions section"""
    return """
    <h2><span class="mw-headline" id="Attractions">Attractions</span></h2>
    <ul>
        <li><a href="/wiki/Statue_of_Liberty">Statue of Liberty</a> - Famous landmark</li>
        <li><a href="/wiki/Central_Park">Central Park</a> - Urban park</li>
        <li><a href="/wiki/Empire_State_Building">Empire State Building</a> - Iconic skyscraper</li>
    </ul>
    """


@pytest.fixture
def sample_climate_html():
    """Sample HTML for climate information"""
    return """
    <h3><span class="mw-headline" id="Climate">Climate</span></h3>
    <p>New York City has a humid subtropical climate (Köppen Cfa), with hot summers and cold winters.
    The city averages 234 days with at least some sunshine annually.</p>
    """


@pytest.fixture
def sample_full_html(
    sample_description_html,
    sample_infobox_html,
    sample_coordinates_html,
    sample_attractions_html,
    sample_climate_html,
):
    """Combines all sample HTML sections into a full Wikipedia page"""
    return f"""
    <html>
    <head><title>New York City - Wikipedia</title></head>
    <body>
        {sample_description_html}
        {sample_infobox_html}
        {sample_coordinates_html}
        {sample_climate_html}
        {sample_attractions_html}
    </body>
    </html>
    """


@pytest.fixture
def sample_soup(sample_full_html):
    """Returns a BeautifulSoup object of the sample HTML"""
    return BeautifulSoup(sample_full_html, "html.parser")


# Define test data
@pytest.fixture
def test_destination_data():
    """Return test destination data"""
    return {
        "destination": "Paris",
        "country": "France",
        "description": "The City of Light",
        "attractions": ["Eiffel Tower", "Louvre Museum"],
        "rating": 4.8,
        "reviews_count": 5000,
        "weather": {"average_temp": 15.5, "best_season": "Spring"},
        "has_beaches": False,
        "updated_at": "2023-06-01T12:00:00Z",
    }


@pytest.fixture
def mock_gcs_client():
    """Create a mock GCS client"""
    with patch("google.cloud.storage.Client") as mock_client:
        yield mock_client


@pytest.fixture
def mock_bq_client():
    """Create a mock BigQuery client"""
    with patch("google.cloud.bigquery.Client") as mock_client:
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        yield mock_client_instance


@pytest.fixture
def mock_storage_bucket():
    """Create a mock GCS bucket"""
    mock_bucket = MagicMock()

    # Set up blob handling
    mock_blob = MagicMock()
    mock_blob.download_as_text.return_value = json.dumps(
        {"destination": "Paris", "country": "France"}
    )

    # Mock return values for blob method with different paths
    def mock_blob_func(blob_name):
        if "destinations" in blob_name:
            return mock_blob
        elif "schemas" in blob_name:
            schema_blob = MagicMock()
            # Breaking long line for schema content
            schema_content = {"fields": [{"name": "destination", "type": "STRING"}]}
            schema_blob.download_as_text.return_value = json.dumps(schema_content)
            return schema_blob
        return mock_blob

    mock_bucket.blob.side_effect = mock_blob_func

    return mock_bucket


@pytest.fixture
def mock_env_vars():
    """Mock environment variables"""
    env_vars = {
        "BQ_PROJECT_ID": "test-project",
        "BQ_STAGING_DATASET_ID": "test_dataset",
        "BQ_DESTINATION_DETAILS_TABLE_ID": "test_destinations",
        "GCS_BUCKET_NAME": "test-bucket",
        "GCS_WEATHER_BUCKET_NAME": "test-weather-bucket",
        "TESTING": "true",
    }

    with patch.dict("os.environ", env_vars):
        yield env_vars
