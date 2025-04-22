# Travel Data Platform

A data engineering platform to process travel data, generate synthetic data, and batch load to BigQuery.

## Architecture

This platform consists of the following main components:

1. **Data Generator**: Creates synthetic travel data based on patterns from the original dataset
2. **BigQuery Ingestion**: Handles data loading and transformations into BigQuery
3. **Pipeline Orchestration**: Coordinates the data generation and ingestion processes
4. **Scheduler**: Provides automated daily runs of the pipeline

## Installation

### Prerequisites

- Python 3.12 or later
- Google Cloud account with BigQuery enabled
- `gcloud` CLI authenticated with appropriate permissions

### Setup

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/travel-data-platform.git
   cd travel-data-platform
   ```

2. Install the package:
   ```
   pip install -e .
   ```

3. Set up Google Cloud credentials:

   The platform provides a utility to help you set up and manage Google Cloud credentials:

   ```
   # View authentication setup options
   python main.py auth --help

   # Set up credentials from a service account key file
   python main.py auth --setup --key-file /path/to/your-service-account-key.json

   # Verify that your credentials work
   python main.py auth --setup --verify
   ```

   Alternative methods:
   
   ```
   # Set environment variable directly
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
   
   # Or provide key file path with each command
   python main.py trips ingest --key-path /path/to/your/service-account-key.json
   ```

### Google Cloud Setup

1. Create a service account in the Google Cloud Console with these roles:
   - BigQuery Data Editor
   - BigQuery Job User

2. Create a service account key (JSON format) and download it

3. Use the key with our authentication system using one of the methods above

## Usage

The platform provides a command-line interface for interacting with the pipeline:

### Generate Data

To generate new synthetic travel data:

```
python main.py trips generate --count 20
```

#### Data Generation Methods

The platform supports two methods for generating synthetic data:

1. **Distribution-based** (default): Uses statistical patterns from the original dataset to generate realistic data that maintains the original distributions.

```
python main.py trips generate --method distribution
```

2. **Faker-based**: Uses the Faker library to generate entirely synthetic data without relying on the original dataset's patterns.

```
python main.py trips generate --method faker
```

Choose the method based on your needs:
- Use **distribution** when you want data that closely resembles the statistical properties of your original dataset.
- Use **faker** when you want more diverse data or don't have a representative original dataset.

### Ingest Data to BigQuery

To ingest all pending CSV files to BigQuery:

```
python main.py trips ingest
```

With custom authentication:

```
python main.py trips ingest --key-path /path/to/key.json --project-id your-project-id
```

### Run Full Pipeline

To run the complete pipeline (generate and ingest):

```
python main.py trips pipeline
```

You can also run only part of the pipeline:

```
python main.py trips pipeline --generate-only
python main.py trips pipeline --ingest-only
```

You can specify which data generation method to use:

```
python main.py trips pipeline --method faker
```

### Schedule Pipeline

To schedule the pipeline to run daily:

```
python main.py trips schedule --time 01:00 --run-now
```

You can specify which data generation method to use in the scheduled runs:

```
python main.py trips schedule --time 01:00 --run-now --method faker
```

With custom authentication:

```
python main.py trips schedule --time 01:00 --key-path /path/to/key.json
```

This will schedule the pipeline to run at 1 AM daily and also run it immediately.

## Authentication Management

The platform offers several options for managing Google Cloud authentication:

1. **Environment Variable**: Set `GOOGLE_APPLICATION_CREDENTIALS` to point to your key file
2. **Default Location**: Store your key at `~/.gcp/service-account-key.json`
3. **Explicit Path**: Provide the `--key-path` argument with each command
4. **Authentication Helper**: Use the `auth` command to set up credentials

The system follows this priority order when looking for credentials:
1. Explicitly provided `--key-path` in the command
2. `GOOGLE_APPLICATION_CREDENTIALS` environment variable
3. Default location at `~/.gcp/service-account-key.json`

## Directory Structure

```
travel-data-platform/
├── pipelines/
│   ├── common/
│   │   ├── __init__.py
│   │   ├── gcp_auth.py        # Authentication utilities
│   │   └── setup_credentials.py # Credential setup helper
│   ├── trips_data/
│   │   ├── data/
│   │   │   ├── raw/         # Raw generated CSV files
│   │   │   └── processed/   # Files that have been processed
│   │   ├── __init__.py
│   │   ├── config.py        # Configuration parameters
│   │   ├── data_generator.py # Distribution-based data generation
│   │   ├── faker_data_generator.py # Faker-based data generation
│   │   ├── bigquery_ingestion.py # BigQuery loading
│   │   ├── pipeline.py      # Pipeline orchestration
│   │   └── scheduler.py     # Scheduling functionality
│   └── weather_api_pipeline/ # Other pipelines (future)
└── main.py                  # CLI entry point
```

## Data Engineering Practices

This project implements several data engineering best practices:

1. **Modular Design**: Each component is separated into its own module with clear responsibilities
2. **Configuration Management**: Centralized configuration in a dedicated module
3. **Logging**: Comprehensive logging for monitoring and debugging
4. **Error Handling**: Robust error handling throughout the pipeline
5. **Data Validation**: Transformation and validation of data before loading
6. **Idempotent Operations**: Files are moved after processing to prevent duplicate ingestion
7. **Metadata Capture**: Each record is tagged with processing metadata
8. **Graceful Termination**: Signals are properly handled for clean shutdowns
9. **Secure Authentication**: Multiple secure methods for handling service account keys

## License

This project is licensed under the MIT License - see the LICENSE file for details.