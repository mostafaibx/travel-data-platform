# Modular Data Engineering Portfolio

A comprehensive data engineering platform that simulates a startup's data infrastructure, integrating multiple data sources into a centralized data warehouse with analytics capabilities.

## Overview

This portfolio project demonstrates end-to-end data engineering skills through a modular architecture that processes data from various sources:

- **API Integration**: Real-time and batch data collection from public APIs
- **Flat File Processing**: Ingestion and transformation of CSV, JSON, and parquet files
- **Web Scraping**: Automated extraction of structured data from websites
- **Database Connectors**: Integration with SQL and NoSQL databases

All data sources feed into a centralized cloud data warehouse that follows dimensional modeling principles, enabling comprehensive analytics and business intelligence.

## Architecture

The platform consists of several interconnected components:

1. **Data Ingestion Layer**: 
   - Multiple independent pipelines for different data sources
   - Support for both batch and streaming ingestion patterns
   - Fault-tolerant design with retry mechanisms and logging

2. **Data Storage Layer**:
   - Cloud data warehouse (BigQuery) as the central repository
   - Data lake for raw data storage
   - Dimensional modeling with both star and snowflake schemas

3. **Transformation Layer**:
   - ELT (Extract, Load, Transform) approach
   - Comprehensive data quality checks
   - Change data capture for incremental processing

4. **Orchestration Layer**:
   - Workflow coordination using Apache Airflow
   - Dependency management between pipelines
   - Monitoring and alerting

5. **Visualization Layer**:
   - Interactive dashboards built with Looker Studio
   - Business-focused KPI reporting

## Data Sources

The platform integrates these data sources:

1. **Travel API Pipeline**: 
   - Real-time flight and hotel pricing data
   - Weather information affecting travel patterns
   - Historical travel trends

2. **OpenWeather API**:
   - 
   - 

3. **Travler Trip Dataset**: 
   - Generate synthatic data from the Travler Trip ds
   - Handle CSV file in batch ingestion

 To Be Done .....

## Data Warehouse Design

The warehouse will follow best practices in setting different stages of data eg. staging, dwh, etc..
It will be implmented in Star Schema, following best practices as mentioned in dbt course.

To Be Done: 
   - Add details doc for dwh planning, archeticture, descisions, etc...

## Technologies Used

- **Data Ingestion**: Python
- **Storage**: Google BigQuery, Google Cloud Storage
- **Processing**: Apache Spark, dbt
- **Orchestration**: Apache Airflow
- **Visualization**: Looker Studio, Tableau
- **Infrastructure**: Google Cloud Platform, Docker, Terraform
- **CI/CD**: GitHub Actions

## Getting Started

### Prerequisites

- Python 3.8 or later
- Google Cloud account with BigQuery enabled
- Docker and Docker Compose
- Terraform (optional, for infrastructure deployment)

### Setup

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/data-engineering-portfolio.git
   cd data-engineering-portfolio
   ```

2. Set up the environment:
   ```
   pip install -e .
   ```

3. Configure credentials:
   ```
   # Set up Google Cloud credentials
   python main.py auth --setup --key-file /path/to/your-service-account-key.json
   ```

4. Start the infrastructure:
   ```
   docker-compose up -d
   ```

## Usage Examples

### Run a Single Pipeline

To run the travel data pipeline:

```
python main.py travel pipeline --run-complete
```


## Directory Structure

```
data-engineering-portfolio/
├── pipelines/
│   ├── common/                   # Shared utilities
│   ├── trips_data/               # Synthatic CSV trips data
│   └── weather_api/              # Weather data pipeline
├── warehouse/
│   ├── schemas/                  # DDL for warehouse tables
│   ├── dbt/                      # dbt models for transformations
│   └── views/                    # SQL views for reporting
├── airflow/
│   └── dags/                     # Airflow DAG definitions
├── infrastructure/
│   └── terraform/                # IaC for cloud resources
├── dashboards/
│   └── looker_studio/            # Dashboard templates
└── main.py                       # CLI entry point
```

## Testing Strategy

This project follows a comprehensive testing strategy to ensure pipeline reliability and code quality:

### Unit Testing

1. **Mock-based testing**: All external dependencies (GCP services, APIs) are mocked during testing
   - Uses Python's `unittest.mock` to replace real service clients
   - Custom mock fixtures in `conftest.py` files
   - No actual cloud resources are accessed during tests

2. **Configuration isolation**:
   - Separate test configuration in `config_test.py` files
   - Environment variables override production values in test environment
   - `TESTING=true` flag triggers test-specific code paths

3. **Fixtures and test data**:
   - Standardized test fixtures provide consistent test data
   - Sample datasets for each pipeline to ensure predictable results

### Integration Testing

1. **Dry run mode**: Pipelines can be executed in dry-run mode to validate logic without external dependencies
2. **Schema validation**: Tests validate that output schemas match expected structure
3. **Data validation**: Tests for data quality, transformations, and business logic

### Test Structure

Each pipeline module has its own test suite:

```
pipelines/pipeline_name/
├── tests/
│   ├── conftest.py              # Test fixtures and configuration
│   ├── config_test.py           # Test-specific configuration values
│   ├── test_fetcher.py          # Tests for data fetching components
│   ├── test_transformer.py      # Tests for data transformation components
│   └── run_tests.py             # Test runner with coverage reporting
```

## CI/CD Implementation

The project uses GitHub Actions for continuous integration and delivery:

### CI Workflow (.github/workflows/data-pipeline-ci.yml)

1. **Dependency Management**:
   - All dependencies are defined in `pyproject.toml` under appropriate extras
   - CI workflow installs with `pip install ".[dev,test]"` to ensure consistency
   - No individual package installations in CI steps

2. **Pipeline Testing Stages**:
   - **Linting**: Code style and quality checks (black, isort, flake8)
   - **Unit Testing**: Testing of all pipeline components
   - **Data Validation**: Validation of data schemas and transformations
   - **Dry Run**: Test pipeline execution without external dependencies
   - **Security Scanning**: Checks for vulnerabilities and security issues

3. **Test Environment**:
   - Environment variables provide mock config values
   - PYTHONPATH configuration ensures proper imports
   - Testing flag enables mock implementations of external services

4. **Artifacts**:
   - Test results published as JUnit XML
   - Coverage reports generated and published
   - Security scan results published for review

### Benefits

This CI/CD approach ensures:
- Consistent testing across all environments
- No dependency on external resources during testing
- Fast feedback on code quality and functionality
- Comprehensive validation before merging to main branches

## Learning Outcomes

This portfolio demonstrates competency in:

1. **Data Engineering Fundamentals**: ETL/ELT processes, data warehousing, dimensional modeling
2. **Cloud Infrastructure**: Scalable data solutions on GCP
3. **Modern Data Stack**: Integration of current tools and practices
4. **Data Quality**: Implementation of testing and validation
5. **Production-Grade Code**: Error handling, logging, documentation
6. **DevOps Practices**: CI/CD, infrastructure as code, containerization

## Next Steps

Planned enhancements include:

- Streaming data processing with Kafka and Spark Streaming
- Implementation of data quality monitoring with Great Expectations
- Machine learning pipeline integration
- Enhanced security features and role-based access

## License

This project is licensed under the MIT License - see the LICENSE file for details.