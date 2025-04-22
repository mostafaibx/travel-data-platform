# Travel Data Pipeline

A showcase data engineering pipeline that simulates the data ingestion process for travel data analytics.

## Overview

This pipeline mimic real-life scenario of batch processing csv and push them to to BigQuery. it generates synthatic data to simulate new csv files comming from business process and automate the ingestion into the data warehouse.

## Features

- **Synthetic Data Generation**: Creates realistic travel data using statistical distributions derived from a sample dataset
- **Data Cleaning & Transformation**: Standardizes formats, handles missing values, and prepares data for analytics
- **BigQuery Integration**: Automatically uploads processed data to Google BigQuery for analysis
- **Configurable Pipeline**: Supports various execution modes and generation methods
- **Logging & Metrics**: Tracks pipeline performance and execution statistics

## Implementation

The pipeline is structured into modular components:

- `data_generator.py`: Generates synthetic travel data using statistical distributions
- `data_cleaner.py`: Cleans and standardizes the raw data
- `bigquery_ingestion.py`: Handles the BigQuery upload process
- `pipeline.py`: Orchestrates the entire workflow
- `config.py`: Centralizes configuration parameters

### Data Generation

The data generator creates synthetic travel data based on the patterns and distributions observed in the original dataset:

- Analyzes the original data to understand patterns in:
  - Trip durations
  - Traveler ages
  - Cost distributions
  - Common destinations and transportation types
- Generates new records with realistic variations while maintaining the statistical properties of the original dataset
- Creates a mix of past and future travel dates to simulate real-world booking patterns
- Adds a daily timestamp to each generated file for tracking

We can also generate data using faker lib through `faker_data_generator.py` through using this flag --method faker

### Data Cleaning

The data cleaner handles standardization and preparation:

- Handles missing values
- Converts cost columns to numeric (removing currency symbols)
- Ensures consistent date formats
- Standardizes traveler demographic information

### Data Ingestion

The BigQuery ingestion module handles loading the data into BigQuery with proper transformations:

- Automatically creates dataset and table if they don't exist
- Handles data type conversions (strings to dates, currency strings to floats)
- Adds metadata columns (ingestion timestamp, source file) for lineage tracking
- Implements idempotent operations by moving processed files to a separate directory

## Data Generation Approaches

This pipeline implements two different data generation strategies that can be selected at runtime:

### Distribution-Based Generation (Default)

- **Statistical Accuracy**: Generated data closely resembles the original dataset's statistical properties
- **Realistic Relationships**: Maintains the correlations and relationships between fields
- **Domain Authenticity**: Produces travel data that looks like real-world bookings
- **Implementation**: Analyzes the original dataset to extract distributions and samples from these distributions

### Faker-Based Generation (Alternative)

- **Independence**: Doesn't require an original dataset to function
- **Diversity**: Can generate more diverse and varied data
- **Flexibility**: Easier to extend to new fields or data types
- **Implementation**: Uses Faker library with predefined lists of destinations, accommodation types, and transportation methods

## Architecture

```
              ┌────────────┐
              │ Original   │
              │ Dataset    │
              └─────┬──────┘
                    │
                    ▼
┌───────────────────────────────┐
│ Data Generation               │
│                               │
│ - Statistical distributions   │
│ - Realistic patterns          │
│ - Configurable batch size     │
└─────────────┬─────────────────┘
              │
              ▼
┌───────────────────────────────┐
│ Data Cleaning                 │
│                               │
│ - Format standardization      │
│ - Missing value handling      │
│ - Type conversion             │
└─────────────┬─────────────────┘
              │
              ▼
┌───────────────────────────────┐
│ BigQuery Ingestion            │
│                               │
│ - Schema mapping              │
│ - Metadata enrichment         │
│ - Batch processing            │
└───────────────────────────────┘
```

## Design Rationale

### Why Batch Processing?

The pipeline implements batch processing because:
- It's simpler to implement and debug
- It's sufficient for the current data volumes
- It creates clear file boundaries for tracking and auditing
- It enables easy reprocessing of specific batches if needed

For higher volumes or near-real-time requirements, streaming pipelines using Pub/Sub and Dataflow would be more appropriate.

### Processing Approach

For production environments, this pipeline could be extended with:
- Cloud Scheduler + Cloud Functions for more robust execution
- Airflow or other workflow managers for better monitoring and retry capabilities
- Kubernetes CronJobs for containerized execution

## Usage

Run the complete pipeline:
```
python -m pipelines.trips_data.pipeline
```

Generate data only:
```
python -m pipelines.trips_data.pipeline --generate-only
```

Ingest existing data only:
```
python -m pipelines.trips_data.pipeline --ingest-only
```

Select generation method:
```
python -m pipelines.trips_data.pipeline --method faker
```

## Skills Demonstrated

- Data Engineering Principles
- ETL Pipeline Design
- Python for Data Processing
- Google Cloud Platform Integration
- Statistical Data Generation
- Configuration Management
- Error Handling & Logging