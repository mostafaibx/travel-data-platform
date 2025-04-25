def fetcher_test():
    """
    Test the fetcher function.
    """
    from pipelines.weather_api_pipeline.fetcher import fetcher

    # Test with a valid city name
    city = "London"
    result = fetcher(city)
    assert isinstance(result, dict)
    assert "weather" in result
    assert "main" in result
    assert "wind" in result

    # Test with an invalid city name
    city = "InvalidCity"
    result = fetcher(city)
    assert isinstance(result, dict)
    assert "error" in result
    assert result["error"] == "City not found" 

    # Test with an empty city name
    city = "" 
    result = fetcher(city)
    assert isinstance(result, dict)
    assert "error" in result
    assert result["error"] == "City name cannot be empty"
    # Test with a city name that contains special characters
    city = "New York"
    result = fetcher(city)
    assert isinstance(result, dict)
    assert "weather" in result
    assert "main" in result
    assert "wind" in result
    # Test with a city name that contains numbers
    city = "12345"
    result = fetcher(city)
    assert isinstance(result, dict)
    assert "error" in result
    assert result["error"] == "City not found"
    # Test with a city name that contains spaces
    city = "San Francisco"
    result = fetcher(city)
    assert isinstance(result, dict)
    assert "weather" in result
    assert "main" in result
    assert "wind" in result
    # Test with a city name that contains special characters
    city = "São Paulo"
    result = fetcher(city)
    assert isinstance(result, dict)
    assert "weather" in result
    assert "main" in result
    assert "wind" in result