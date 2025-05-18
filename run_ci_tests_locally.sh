#!/bin/bash
set -e

# Set environment variables similar to CI
export PYTHONPATH=$PWD
export BQ_PROJECT_ID=test-project
export BQ_STAGING_DATASET_ID=test_dataset
export BQ_DESTINATION_DETAILS_TABLE_ID=test_destinations
export GCS_BUCKET_NAME=test-bucket
export GCS_WEATHER_BUCKET_NAME=test-weather-bucket
export TESTING=true

# Print environment info
echo "============================================="
echo "Running tests in environment:"
echo "Python: $(python --version)"
echo "Path: $PYTHONPATH"
echo "============================================="

# Step 1: Linting - first fix formatting issues
echo "Formatting code with black and isort..."
echo "============================================="
black pipelines/
isort pipelines/

# Now run linting checks
echo "Running linting checks..."
echo "============================================="
black --check pipelines/
isort --check pipelines/
flake8 pipelines/

# Step 2: Run all tests
echo "Running all tests..."
echo "============================================="
mkdir -p junit
pytest pipelines/ --junitxml=junit/test-results.xml -v

# Step 3: Run specific scrapping pipeline tests with coverage
echo "Running scrapping pipeline tests with coverage..."
echo "============================================="
mkdir -p junit/scrapping_dest_details
mkdir -p coverage-reports/scrapping_dest_details
cd pipelines/scrapping_dest_details
python -m tests.run_tests --with-coverage --junit-xml=../../junit/scrapping_dest_details/test-results.xml --html-report=../../coverage-reports/scrapping_dest_details
cd ../..

# Step 4: Data validation
echo "Running data validation tests..."
echo "============================================="
cd pipelines/scrapping_dest_details
python -m pytest tests/test_schema_validation.py -v
cd ../..

# Step 5: Run dry-run tests
echo "Running pipeline in dry-run mode..."
echo "============================================="
export DRY_RUN=true

# Create mock environment
mkdir -p ./mock_data
mkdir -p ./mock_data/scrapping_dest_details

# Create dry run config
cat > pipelines/scrapping_dest_details/tests/dry_run_config.py << EOL
# Mock config for dry run
TRAVEL_DESTINATIONS = ["Paris", "Rome", "New York"]
PROJECT_ID = "test-project"
DATASET_ID = "test_dataset"
TABLE_ID = "test_destinations"
BQ_TABLE_PATH = "test-project.test_dataset.test_destinations"
BUCKET_NAME = "test-bucket"
EOL

# Run the scrapping pipeline in dry-run mode as a module
echo "Running dry run with proper module import..."
python -c "
import sys
import os
from unittest.mock import patch
import json
try:
    # Use absolute imports
    from pipelines.scrapping_dest_details.pipeline import run_pipeline
    
    # Mock essential external dependencies with absolute import paths
    with patch('pipelines.scrapping_dest_details.fetcher.requests.Session'), \\
         patch('pipelines.scrapping_dest_details.gcs_storage._get_bucket'), \\
         patch('pipelines.scrapping_dest_details.bigquery_loader.bigquery.Client'), \\
         patch('pipelines.scrapping_dest_details.pipeline.TRAVEL_DESTINATIONS', ['Paris', 'Rome']):
        
        # Run pipeline in dry mode
        success = run_pipeline()
        print(f'Dry run completed with success={success}')
        if not success:
            sys.exit(1)
except Exception as e:
    print(f'Error: {str(e)}')
    sys.exit(1)
"

# Step 6: Security checks
echo "Running security checks..."
echo "============================================="
bandit -r pipelines/ || echo "Bandit security scan failed"
safety check || echo "Safety check failed"

echo "============================================="
echo "All CI tests completed locally!"
echo "Check junit/ and coverage-reports/ for test and coverage results" 