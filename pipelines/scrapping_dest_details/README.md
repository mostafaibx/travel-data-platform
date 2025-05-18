# Wikipedia Destination Details Scraping Pipeline

This pipeline extracts travel destination information from Wikipedia, processes it, and loads it into Google Cloud Storage (GCS) and BigQuery for analysis.

## Architecture Overview

The pipeline follows a modular design with clear separation of concerns:

```
                           ┌───────────────────┐
                           │                   │
                           │    Wikipedia      │
                           │                   │
                           └─────────┬─────────┘
                                     │
                                     ▼
                           ┌───────────────────┐
                           │                   │
                           │  WikipediaScraper │
                           │    (fetcher.py)   │
                           │                   │
                           └─────────┬─────────┘
                                     │
                                     ▼
┌───────────────────┐      ┌───────────────────┐      ┌───────────────────┐
│                   │      │                   │      │                   │
│ Raw Data Storage  │◄─────┤  Pipeline Logic   │─────►│ Processed Storage │
│  (gcs_storage.py) │      │  (pipeline.py)    │      │(bigquery_loader.py)│
│                   │      │                   │      │                   │
└───────────────────┘      └───────────────────┘      └───────────────────┘
         │                                                      │
         ▼                                                      ▼
┌───────────────────┐                             ┌───────────────────┐
│                   │                             │                   │
│ Google Cloud      │                             │    BigQuery       │
│    Storage        │                             │                   │
│                   │                             │                   │
└───────────────────┘                             └───────────────────┘
```

## Key Components

### 1. Fetcher (`fetcher.py`)

The core component responsible for data extraction:

- **WikipediaScraper Class**: Object-oriented approach for better organization
- **Rate Limiting**: Respectful scraping with configurable delays
- **Rich Data Extraction**:
  - Destination descriptions
  - Geographic coordinates
  - Population statistics
  - Climate information
  - Tourist attractions
  - Images and metadata
- **Error Handling**: Robust exception handling for production reliability
- **Parallel Processing**: Uses ThreadPoolExecutor for efficient concurrent scraping

### 2. Data Storage

#### Raw Data (`gcs_storage.py`)

- **Data Lineage**: Preserves raw HTML content with metadata
- **Versioning**: Timestamps in filenames for tracking historical changes
- **Separation of Concerns**: 
  - Metadata stored as JSON for queryability
  - Full HTML content stored separately to reduce costs
  - Efficient organization with prefixes

#### Processed Data (`bigquery_loader.py`)

- **Schema Management**: Explicit schema definition
- **Efficient Merging**: MERGE operations to handle updates elegantly
- **Temporary Tables**: Uses staging tables for data validation
- **Error Handling**: Graceful failure and logging
- **Modular Design**: Reusable BigQueryLoader class

### 3. Pipeline Orchestration (`pipeline.py`)

- **Main Workflow**: Coordinates the full ETL process
- **Parallel Processing**: Manages concurrent data extraction
- **Data Transformation**: Prepares data for storage
- **Error Handling**: Comprehensive error handling and logging

## Data Flow

1. **Extraction**:
   - Fetcher retrieves Wikipedia pages for configured destinations
   - Extracts structured data using BeautifulSoup

2. **Raw Storage**:
   - Raw HTML stored in GCS with metadata
   - Uses versioning with timestamps

3. **Transformation**:
   - Data normalized and prepared for BigQuery
   - JSON arrays used for nested data (attractions, languages)

4. **Loading**:
   - Data uploaded to BigQuery using MERGE operations
   - Efficient update mechanism for incremental loads

## Data Model

### Destinations Table

| Field | Type | Description |
|-------|------|-------------|
| destination_name | STRING | Primary key - Name of the destination |
| description | STRING | First paragraph description |
| country | STRING | Country where the destination is located |
| latitude | FLOAT | Geographic latitude |
| longitude | FLOAT | Geographic longitude |
| population_count | INTEGER | Population count |
| population_year | INTEGER | Year of the population data |
| timezone | STRING | Timezone information |
| languages | STRING | JSON array of languages spoken |
| climate | STRING | Climate description |
| image_url | STRING | URL of the main image |
| sections | STRING | JSON array of main section titles |
| area_km2 | FLOAT | Area in square kilometers |
| region | STRING | Region/state information |
| attractions_count | INTEGER | Number of attractions found |
| attractions | STRING | JSON array of all attractions |
| ingestion_timestamp | TIMESTAMP | When the data was ingested |

## Data Engineering Best Practices

### 1. Separation of Concerns
The codebase is modularly structured with distinct responsibilities:
- **fetcher.py**: Data extraction
- **gcs_storage.py**: Raw data storage
- **bigquery_loader.py**: Processed data storage
- **pipeline.py**: Orchestration
- **config.py**: Configuration

### 2. Error Resilience
- Comprehensive exception handling
- Graceful degradation when individual destinations fail
- Detailed logging for monitoring and debugging

### 3. Data Lineage
- Raw data preserved for auditability
- Timestamps for tracking historical changes
- Metadata stored alongside content

### 4. Idempotent Operations
- MERGE operations ensure safe re-runs
- Temporary tables prevent partial updates
- Versioned storage prevents data loss

### 5. Extensibility
- Object-oriented design for easy expansion
- Configuration-driven approach
- Clear interface boundaries between components

### 6. Efficiency
- Parallel processing with ThreadPoolExecutor
- Batched operations for better performance
- Optimized storage patterns (metadata separate from content)

### 7. Responsible Scraping
- Configurable rate limiting
- Custom user-agent for identification
- Minimal necessary data extraction

## Configuration

The pipeline is configured through `config.py`:

- **Project Settings**: BigQuery project, dataset, and table IDs
- **Source Management**: List of destinations to scrape
- **Storage Settings**: GCS bucket name and prefixes

Environment variables can be set through a `.env` file:
- `BQ_PROJECT_ID`: Google Cloud project ID
- `BQ_STAGING_DATASET_ID`: BigQuery dataset ID
- `BQ_DESTINATION_DETAILS_TABLE_ID`: BigQuery table ID
- `GCS_BUCKET_NAME`: GCS bucket name (default: "travel-data-raw")

## Usage

### Basic Usage

```python
from pipelines.scrapping_dest_details import run_pipeline

# Run the pipeline
success = run_pipeline()

if success:
    print("Pipeline completed successfully")
else:
    print("Pipeline encountered errors")
```

### Command Line Usage

```bash
python -m pipelines.scrapping_dest_details
```

### Custom Destination List

```python
import os
from pipelines.scrapping_dest_details import run_pipeline
from pipelines.scrapping_dest_details.config import TRAVEL_DESTINATIONS

# Add custom destinations
os.environ["CUSTOM_DESTINATIONS"] = "true"
TRAVEL_DESTINATIONS.extend([
    "Amsterdam",
    "Berlin",
    "Venice"
])

# Run with extended destination list
run_pipeline()
```

## Monitoring and Logging

The pipeline uses Python's logging module with structured logging:
- INFO level for normal operation
- WARNING for non-fatal issues
- ERROR for failures
- DEBUG for detailed troubleshooting

## Testing

The pipeline includes comprehensive tests to ensure data quality, reliability, and robustness.

### Test Structure

Tests are organized by module:

- **test_fetcher.py**: Validates the Wikipedia scraping functionality
- **test_gcs_storage.py**: Tests the GCS storage operations
- **test_bigquery_loader.py**: Verifies BigQuery loading operations
- **test_pipeline.py**: Tests the main pipeline integration
- **test_data_quality.py**: Ensures data meets quality standards

### Testing Best Practices

The test suite follows data engineering best practices:

1. **Modular Testing**: Each component has isolated tests
2. **Mocking External Services**: GCS and BigQuery operations are mocked
3. **Test Data**: Mock HTML and sample data fixtures for consistent tests
4. **Data Quality Validation**: Explicit tests for data integrity
5. **Edge Cases**: Tests handle empty data, invalid inputs, and special characters
6. **Integration Testing**: Verifies components work together correctly

### Running Tests

Install development dependencies:

```bash
pip install -r requirements-dev.txt
```

Run the test suite:

```bash
pytest pipelines/scrapping_dest_details/tests/
```

Run with coverage:

```bash
pytest --cov=pipelines.scrapping_dest_details pipelines/scrapping_dest_details/tests/
```

### Continuous Integration

We recommend integrating these tests into your CI/CD pipeline to ensure code changes don't break functionality.

## Development

For development:

1. Install development dependencies:
   ```bash
   pip install -r requirements-dev.txt
   ```

2. Format code with Black:
   ```bash
   black pipelines/
   ```

3. Run linting:
   ```bash
   flake8 pipelines/
   ```

4. Run type checking:
   ```bash
   mypy pipelines/
   ```

## Future Enhancements

Potential improvements:
- Add more data quality checks
- Implement incremental scraping based on page changes
- Add a scheduler for regular updates
- Expand attraction details extraction
- Implement sentiment analysis on descriptions
- Add additional data sources for enrichment

## Requirements

Dependencies are listed in `requirements.txt`:
- beautifulsoup4
- requests
- google-cloud-storage
- google-cloud-bigquery
- pandas
- python-dotenv 