"""
Tests for the fetcher module in the scrapping_dest_details pipeline.
"""

from unittest.mock import MagicMock, patch

from ..fetcher import WikipediaScraper, get_destination_info


class TestWikipediaScraper:
    """Tests for the WikipediaScraper class"""

    def test_init(self):
        """Test initialization of WikipediaScraper"""
        scraper = WikipediaScraper()
        assert scraper.rate_limit_delay == 1.0

        # Test with custom rate limit
        custom_scraper = WikipediaScraper(rate_limit_delay=2.0)
        assert custom_scraper.rate_limit_delay == 2.0

    @patch("requests.Session")
    def test_fetch_page(self, mock_session_class, mock_requests_session):
        """Test fetching a Wikipedia page"""
        mock_session_instance = mock_requests_session
        mock_session_class.return_value = mock_session_instance

        scraper = WikipediaScraper(rate_limit_delay=0)  # Set to 0 for faster tests
        soup, raw_data = scraper.fetch_page("Paris")

        # Check that the session's get method was called with the correct URL
        mock_session_instance.get.assert_called_once_with(
            "https://en.wikipedia.org/wiki/Paris"
        )

        # Verify returned data
        assert soup is not None
        assert raw_data is not None
        assert "url" in raw_data
        assert "status_code" in raw_data
        assert "headers" in raw_data
        assert "content" in raw_data
        assert "encoding" in raw_data

    @patch("requests.Session")
    def test_fetch_page_error(self, mock_session_class):
        """Test handling of errors when fetching a Wikipedia page"""
        mock_session = MagicMock()
        mock_session.get.side_effect = Exception("Connection error")
        mock_session_class.return_value = mock_session

        scraper = WikipediaScraper(rate_limit_delay=0)
        soup, raw_data = scraper.fetch_page("NonExistentPage")

        # Verify error handling
        assert soup is None
        assert raw_data is None

    def test_extract_destination_info(self, mock_soup):
        """Test extraction of destination information from Wikipedia page"""
        scraper = WikipediaScraper()
        info = scraper.extract_destination_info(mock_soup, "Paris")

        # Check basic info extraction
        assert info["destination_name"] == "Paris"
        assert "capital and most populous city of France" in info["description"]
        assert info["country"] == "France"

        # Check coordinates
        assert info["coordinates"]["latitude"] == 48.8566
        assert info["coordinates"]["longitude"] == 2.3522

        # Check population
        assert info["population"]["count"] == 2148271
        assert info["population"]["year"] == 2020

        # Check other fields
        assert info["timezone"] == "UTC+1 (CET)"
        assert "French" in info["languages"]
        assert "oceanic climate" in info["climate"]

        # Check attractions
        assert len(info["attractions"]) == 2
        assert any(
            attraction["name"] == "Eiffel Tower" for attraction in info["attractions"]
        )
        assert any(
            attraction["name"] == "Louvre Museum" for attraction in info["attractions"]
        )

    def test_extract_description(self, mock_soup):
        """Test extraction of description from Wikipedia page"""
        scraper = WikipediaScraper()
        description = scraper._extract_description(mock_soup)
        assert "Paris is the capital and most populous city of France" in description

    def test_extract_coordinates(self, mock_soup):
        """Test extraction of coordinates from Wikipedia page"""
        scraper = WikipediaScraper()
        coords = scraper._extract_coordinates(mock_soup)
        assert coords["latitude"] == 48.8566
        assert coords["longitude"] == 2.3522

    def test_extract_country(self, mock_soup):
        """Test extraction of country from Wikipedia page"""
        scraper = WikipediaScraper()
        country = scraper._extract_country(mock_soup)
        assert country == "France"

    def test_extract_population(self, mock_soup):
        """Test extraction of population information from Wikipedia page"""
        scraper = WikipediaScraper()
        population = scraper._extract_population(mock_soup)
        assert population["count"] == 2148271
        assert population["year"] == 2020

    def test_extract_timezone(self, mock_soup):
        """Test extraction of timezone from Wikipedia page"""
        scraper = WikipediaScraper()
        timezone = scraper._extract_timezone(mock_soup)
        assert timezone == "UTC+1 (CET)"

    def test_extract_languages(self, mock_soup):
        """Test extraction of languages from Wikipedia page"""
        scraper = WikipediaScraper()
        languages = scraper._extract_languages(mock_soup)
        assert "French" in languages

    def test_extract_climate(self, mock_soup):
        """Test extraction of climate information from Wikipedia page"""
        scraper = WikipediaScraper()
        climate = scraper._extract_climate(mock_soup)
        assert "oceanic climate" in climate


@patch("pipelines.scrapping_dest_details.fetcher.WikipediaScraper")
def test_get_destination_info(mock_scraper_class, mock_soup, mock_raw_data):
    """Test the get_destination_info function"""
    # Setup mock scraper
    mock_scraper_instance = MagicMock()
    mock_scraper_class.return_value = mock_scraper_instance

    # Mock the fetch_page method to return our test data
    mock_scraper_instance.fetch_page.return_value = (mock_soup, mock_raw_data)

    # Mock the extract_destination_info method
    expected_info = {
        "destination_name": "Paris",
        "description": "Paris is the capital and most populous city of France.",
        "coordinates": {"latitude": 48.8566, "longitude": 2.3522},
        "country": "France",
        "population": {"count": 2148271, "year": 2020},
        "timezone": "UTC+1 (CET)",
        "languages": ["French"],
        "climate": "Paris has a typical Western European oceanic climate.",
        "image_url": "https://example.com/paris.jpg",
        "sections": ["History", "Climate", "Landmarks"],
        "attractions": [
            {"name": "Eiffel Tower", "description": "Iconic tower", "type": "Landmark"}
        ],
    }
    mock_scraper_instance.extract_destination_info.return_value = expected_info

    # Call the function
    result_info, result_raw_data = get_destination_info("Paris")

    # Verify
    assert result_info == expected_info
    assert result_raw_data == mock_raw_data
    mock_scraper_instance.fetch_page.assert_called_once_with("Paris")
    mock_scraper_instance.extract_destination_info.assert_called_once_with(
        mock_soup, "Paris"
    )


@patch("pipelines.scrapping_dest_details.fetcher.WikipediaScraper")
def test_get_destination_info_failure(mock_scraper_class):
    """Test the get_destination_info function when fetching fails"""
    # Setup mock scraper to return None
    mock_scraper_instance = MagicMock()
    mock_scraper_class.return_value = mock_scraper_instance
    mock_scraper_instance.fetch_page.return_value = (None, None)

    # Call the function
    result_info, result_raw_data = get_destination_info("NonExistentPage")

    # Verify returns None for both
    assert result_info is None
    assert result_raw_data is None
