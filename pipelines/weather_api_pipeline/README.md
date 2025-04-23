# Weather API Pipeline - Technical Documentation

## Overview

The Weather API Pipeline is a data engineering solution designed to collect weather forecast data for popular travel destinations from the OpenWeather API. It processes this data and stores it in both Google BigQuery for analytics and Google Cloud Storage for data lineage. This pipeline is designed to run daily, providing travel agencies with up-to-date weather information for key tourist destinations.

## Architecture

```
                   ┌─────────────────┐
                   │                 │
                   │  OpenWeather    │
                   │  API            │
                   │                 │
                   └────────┬────────┘
                            │
                            ▼
┌───────────────────────────────────────────────┐
│                                               │
│  Weather API Pipeline                         │
│  ┌───────────────┐    ┌────────────────────┐  │
│  │               │    │                    │  │
│  │   Fetcher     │───▶│  Data Processor    │  │
│  │               │    │                    │  │
│  └───────────────┘    └──────┬─────┬──────┘  │
│                              │     │         │
└──────────────────────────────│─────│─────────┘
                               │     │
                               │     │
                    ┌──────────▼─┐   │    ┌──────────────┐
                    │            │   │    │              │
                    │  BigQuery  │   └───▶│  GCS Storage │
                    │            │        │              │
                    └────────────┘        └──────────────┘
```

## Components

The pipeline is organized into several modular components, each with a specific responsibility:

### 1. Fetcher Module (`fetcher.py`)

Responsible for interacting with the OpenWeather API.

- **Key Function**: `get_weather_data(lat, lon, exclude, units, lang)`
- **Features**:
  - Makes HTTP requests to OpenWeather API
  - Supports customization (units, language, data exclusions)
  - Implements error handling and logging
  - Returns complete weather data JSON

### 2. Pipeline Module (`pipeline.py`)

The orchestration component that coordinates the entire ETL process.

- **Key Functions**:
  - `fetch_forecast_data()`: Collects and processes data for all destinations
  - `upload_to_bigquery()`: Loads processed data into BigQuery
  - `run_pipeline()`: Main orchestration function
  
- **Features**:
  - Predefined list of 15 popular travel destinations
  - Extracts and transforms weather data into a structured format
  - Handles API responses and error conditions
  - Creates BigQuery tables with appropriate schema if they don't exist
  - Implements comprehensive logging

### 3. GCS Storage Module (`gcs_storage.py`)

Handles raw data storage in Google Cloud Storage for data lineage.

- **Key Function**: `upload_raw_data_to_gcs(data, raw_responses)`
- **Features**:
  - Implements a custom JSON encoder for datetime objects
  - Creates a timestamp-based folder structure (YYYY/MM/DD)
  - Adds metadata to raw data for tracking and auditing
  - Error handling and logging

### 4. Configuration Module (`config.py`)

Centralizes all configuration parameters for the pipeline.

- **Features**:
  - API credentials and endpoint URLs
  - GCP project, dataset, and table IDs
  - GCS bucket and path configurations

## Data Flow

1. **Data Collection**:
   - The pipeline fetches forecast data for 15 predefined travel destinations
   - For each location, it requests 2 days of forecast data (today and tomorrow)
   - All API responses are stored in their raw form

2. **Data Processing**:
   - Raw JSON responses are transformed into a structured format
   - Key weather metrics relevant for travel are extracted
   - Data is organized with appropriate types and dimensions

3. **Data Storage**:
   - **BigQuery**: Processed data is loaded into a structured table
   - **GCS**: Raw JSON responses with metadata are stored in a date-partitioned structure

## Implementation Details

### API Integration

The pipeline uses the OpenWeather API's "One Call" endpoint, which provides:
- Current weather
- Daily forecasts
- Weather conditions, temperatures, and other metrics

The API integration is designed to be:
- Resilient to API failures
- Configurable in terms of units and data components
- Resource-efficient by avoiding unnecessary data retrieval

### BigQuery Schema

The BigQuery table is designed with a schema that optimizes for:
- Travel-specific analytics
- Efficient querying
- Proper data typing

Key fields include:
- Location data (city, country, coordinates)
- Forecast date and timestamp
- Temperature metrics (max, min, day, night, feels like)
- Weather conditions (main category, description, icon)
- Travel-relevant metrics (humidity, wind, UV index, precipitation)
- Temporal data (sunrise, sunset)
- Data lineage (ingestion timestamp)

### GCS Raw Data Storage

Raw data is stored in GCS following best practices:
- Hierarchical, date-based path structure
- Metadata enrichment
- Standardized file naming

File path format:
```
gs://travel-data-platform-raw/weather_data/raw/YYYY/MM/DD/weather_data_YYYYMMDD.json
```

JSON payload includes:
- Metadata section with provenance information
- Raw API responses for all locations

### Error Handling

The pipeline implements comprehensive error handling:
- Individual location failures don't stop the entire pipeline
- API errors are logged but allow the pipeline to continue
- Database and storage errors are captured and reported
- All errors include detailed logging for troubleshooting

## Running the Pipeline

### Prerequisites

1. Google Cloud Platform account with:
   - BigQuery dataset
   - GCS bucket
   - Proper IAM permissions

2. OpenWeather API key

3. Python 3.7+ with required libraries (see requirements.txt)

### Configuration

1. Set API key and GCP settings in `config.py` or environment variables
2. Ensure GCP authentication is properly set up

### Execution

Run the pipeline via:

```bash
python -m pipelines.weather_api_pipeline.pipeline
```

Or import and run programmatically:

```python
from pipelines.weather_api_pipeline import run_pipeline
run_pipeline()
```

## Best Practices Implemented

### 1. Modularity

The code is organized into logical modules with clear separation of concerns:
- API interaction (fetcher.py)
- Data transformation and orchestration (pipeline.py)
- Storage mechanisms (gcs_storage.py)
- Configuration (config.py)

### 2. Error Handling

- Comprehensive try/except blocks
- Detailed error logging
- Graceful degradation (partial failures don't stop the entire pipeline)

### 3. Logging

- Consistent logging format throughout the codebase
- Different log levels (INFO, WARNING, ERROR)
- Useful context in log messages

### 4. Type Annotations

- Python type hints used throughout the codebase
- Improves code readability and IDE support
- Helps prevent type-related bugs

### 5. Configuration Management

- Centralized configuration
- Environment variable support
- Separation of code and configuration

### 6. Data Integrity

- Raw data preservation for lineage
- Consistent schema enforcement
- Proper data typing

### 7. Resource Efficiency

- Batched BigQuery uploads
- Selective API data retrieval
- Efficient data processing

## Monitoring and Maintenance

### Monitoring Considerations

1. **Pipeline Success/Failure**:
   - The pipeline logs completion status
   - Can be integrated with monitoring systems (not implemented yet)

2. **Data Quality**:
   - Row counts are logged
   - API response validation is performed

### Maintenance Tasks

1. **API Updates**:
   - If the OpenWeather API changes, update the fetcher module

2. **Destination List Updates**:
   - Edit the TRAVEL_DESTINATIONS list in pipeline.py to add/remove locations

3. **Schema Evolution**:
   - If new fields are needed, update the BigQuery schema in pipeline.py

## Future Enhancements

Potential improvements for future versions:

1. **Dynamic Destination List**:
   - Load travel destinations from a configuration file or database

2. **Advanced Error Recovery**:
   - Implement retries for failed API requests
   - Add dead-letter queue for failed records

3. **Performance Optimization**:
   - Parallel API requests for faster data collection
   - Batched BigQuery loading

4. **Monitoring Integrations**:
   - Add integration with monitoring systems
   - Implement alerting for pipeline failures

5. **Data Quality Checks**:
   - Add validation rules for incoming data
   - Implement anomaly detection

## Conclusion

The Weather API Pipeline provides a robust solution for collecting, processing, and storing weather forecast data for travel planning. Its modular design, error handling, and adherence to best practices make it maintainable and extensible for future needs. 