"""
Tests for the fetcher module in the scrapping_dest_details pipeline.
"""

import json
import re
from unittest.mock import MagicMock, patch

import pytest
import requests
from bs4 import BeautifulSoup

from ..fetcher import WikipediaScraper, get_destination_info


class TestWikipediaScraper:
    """Tests for the WikipediaScraper class"""

    def test_init(self):
        """Test initialization of WikipediaScraper"""
        scraper = WikipediaScraper()
        assert scraper.rate_limit_delay == 1.0
        assert isinstance(scraper.session, requests.Session)
        assert "User-Agent" in scraper.session.headers

        # Test with custom rate limit
        custom_scraper = WikipediaScraper(rate_limit_delay=2.0)
        assert custom_scraper.rate_limit_delay == 2.0

    @patch("time.sleep")
    def test_fetch_page_success(self, mock_sleep, mock_response):
        """Test successfully fetching a Wikipedia page"""
        with patch.object(requests.Session, 'get') as mock_get:
            # Set up mock response
            mock_get.return_value = mock_response
            
            # Create scraper and call fetch_page
            scraper = WikipediaScraper(rate_limit_delay=0.5)
            soup, raw_data = scraper.fetch_page("Paris")
            
            # Verify rate limiting was applied
            mock_sleep.assert_called_once_with(0.5)
            
            # Verify session.get was called with correct URL
            mock_get.assert_called_once_with("https://en.wikipedia.org/wiki/Paris")
            
            # Verify returned data
            assert isinstance(soup, BeautifulSoup)
            assert isinstance(raw_data, dict)
            assert "url" in raw_data
            assert "status_code" in raw_data
            assert "headers" in raw_data
            assert "content" in raw_data
            assert "encoding" in raw_data

    def test_fetch_page_error(self):
        """Test handling of errors when fetching a Wikipedia page"""
        with patch.object(requests.Session, 'get') as mock_get:
            # Set up mock to raise exception
            mock_get.side_effect = requests.exceptions.RequestException("Connection error")
            
            # Create scraper and call fetch_page
            scraper = WikipediaScraper(rate_limit_delay=0)
            soup, raw_data = scraper.fetch_page("NonExistentPage")
            
            # Verify error handling
            assert soup is None
            assert raw_data is None
            mock_get.assert_called_once()

    def test_extract_destination_info(self, mock_soup):
        """Test extraction of destination information from Wikipedia page"""
        # Create scraper and extract info
        scraper = WikipediaScraper()
        info = scraper.extract_destination_info(mock_soup, "Paris")
        
        # Verify basic structure
        assert isinstance(info, dict)
        assert "destination_name" in info
        assert "description" in info
        assert "coordinates" in info
        assert "country" in info
        assert "population" in info
        assert "timezone" in info
        assert "languages" in info
        assert "climate" in info
        assert "image_url" in info
        assert "sections" in info
        assert "attractions" in info
        
        # Check specific values
        assert info["destination_name"] == "Paris"
        assert isinstance(info["description"], str)
        assert isinstance(info["coordinates"], dict)
        assert isinstance(info["population"], dict)
        assert isinstance(info["languages"], list)
        assert isinstance(info["sections"], list)
        assert isinstance(info["attractions"], list)
        
        # These values will depend on your mock_soup implementation
        if "latitude" in info["coordinates"] and info["coordinates"]["latitude"] is not None:
            assert -90 <= info["coordinates"]["latitude"] <= 90
        if "longitude" in info["coordinates"] and info["coordinates"]["longitude"] is not None:
            assert -180 <= info["coordinates"]["longitude"] <= 180

    def test_extract_description(self, mock_soup):
        """Test extraction of description from Wikipedia page"""
        scraper = WikipediaScraper()
        description = scraper._extract_description(mock_soup)
        
        # Verify basic structure
        assert isinstance(description, str)
        
        # Check that citations are removed
        assert not re.search(r"\[\d+\]", description)

    def test_extract_coordinates(self, mock_soup):
        """Test extraction of coordinates from Wikipedia page"""
        scraper = WikipediaScraper()
        coords = scraper._extract_coordinates(mock_soup)
        
        # Verify structure
        assert isinstance(coords, dict)
        assert "latitude" in coords
        assert "longitude" in coords
        
        # Check data types - allow for None values in case mock doesn't have coordinates
        if coords["latitude"] is not None:
            assert isinstance(coords["latitude"], float)
        if coords["longitude"] is not None:
            assert isinstance(coords["longitude"], float)

    def test_extract_country(self, mock_soup):
        """Test extraction of country from Wikipedia page"""
        scraper = WikipediaScraper()
        country = scraper._extract_country(mock_soup)
        
        # Verify basic structure
        assert isinstance(country, str)

    def test_extract_population(self, mock_soup):
        """Test extraction of population information from Wikipedia page"""
        scraper = WikipediaScraper()
        population = scraper._extract_population(mock_soup)
        
        # Verify structure
        assert isinstance(population, dict)
        assert "count" in population
        assert "year" in population
        assert "density" in population
        
        # Check data types
        if population["count"] is not None:
            assert isinstance(population["count"], int)
        if population["year"] is not None:
            assert isinstance(population["year"], int)

    def test_extract_languages(self, mock_soup):
        """Test extraction of languages from Wikipedia page"""
        scraper = WikipediaScraper()
        languages = scraper._extract_languages(mock_soup)
        
        # Verify structure
        assert isinstance(languages, list)
        
        # Check all items are strings
        for lang in languages:
            assert isinstance(lang, str)

    def test_extract_climate(self, mock_soup):
        """Test extraction of climate information from Wikipedia page"""
        scraper = WikipediaScraper()
        climate = scraper._extract_climate(mock_soup)
        
        # Verify structure
        assert isinstance(climate, str)

    def test_extract_section_titles(self, mock_soup):
        """Test extraction of section titles from Wikipedia page"""
        scraper = WikipediaScraper()
        sections = scraper._extract_section_titles(mock_soup)
        
        # Verify structure
        assert isinstance(sections, list)
        
        # Check all items are strings
        for section in sections:
            assert isinstance(section, str)

    def test_extract_attractions(self, mock_soup):
        """Test extraction of attractions from Wikipedia page"""
        scraper = WikipediaScraper()
        attractions = scraper._extract_attractions(mock_soup)
        
        # Verify structure
        assert isinstance(attractions, list)
        
        # Check all items are dicts with required keys
        for attraction in attractions:
            assert isinstance(attraction, dict)
            assert "name" in attraction
            assert "description" in attraction
            assert "type" in attraction
            assert isinstance(attraction["name"], str)
            assert isinstance(attraction["description"], str)
            assert isinstance(attraction["type"], str)


@patch("pipelines.scrapping_dest_details.fetcher.WikipediaScraper")
def test_get_destination_info_success(mock_scraper_class, mock_soup, mock_raw_data):
    """Test the get_destination_info function successful case"""
    # Create mock scraper
    mock_scraper = MagicMock()
    mock_scraper_class.return_value = mock_scraper
    
    # Setup mock responses
    mock_scraper.fetch_page.return_value = (mock_soup, mock_raw_data)
    mock_info = {
        "destination_name": "Paris",
        "description": "Test description",
        "coordinates": {"latitude": 48.8566, "longitude": 2.3522},
        "country": "France",
        "population": {"count": 2148271, "year": 2020},
        "timezone": "UTC+1 (CET)",
        "languages": ["French"],
        "climate": "Test climate",
        "image_url": "https://example.com/image.jpg",
        "sections": ["Section1", "Section2"],
        "attractions": [{"name": "Attraction1", "description": "Test", "type": "Type"}]
    }
    mock_scraper.extract_destination_info.return_value = mock_info
    
    # Call the function
    info, raw_data = get_destination_info("Paris")
    
    # Verify
    assert info == mock_info
    assert raw_data == mock_raw_data
    mock_scraper.fetch_page.assert_called_once_with("Paris")
    mock_scraper.extract_destination_info.assert_called_once_with(mock_soup, "Paris")


@patch("pipelines.scrapping_dest_details.fetcher.WikipediaScraper")
def test_get_destination_info_failure(mock_scraper_class):
    """Test the get_destination_info function when fetching fails"""
    # Create mock scraper
    mock_scraper = MagicMock()
    mock_scraper_class.return_value = mock_scraper
    
    # Setup mock to return None for fetch_page
    mock_scraper.fetch_page.return_value = (None, None)
    
    # Call the function
    info, raw_data = get_destination_info("NonExistentPage")
    
    # Verify
    assert info is None
    assert raw_data is None
    mock_scraper.fetch_page.assert_called_once_with("NonExistentPage")
    mock_scraper.extract_destination_info.assert_not_called()


@pytest.fixture
def mock_response():
    """Fixture providing a mock HTTP response"""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"Content-Type": "text/html; charset=UTF-8"}
    mock_resp.text = "<html><body><div id='content'>Test content</div></body></html>"
    mock_resp.encoding = "UTF-8"
    mock_resp.raise_for_status.return_value = None
    return mock_resp
