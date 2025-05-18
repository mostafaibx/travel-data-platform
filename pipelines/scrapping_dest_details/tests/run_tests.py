#!/usr/bin/env python3
"""
Test runner for the scrapping_dest_details pipeline.
Used by the CI pipeline and can be run locally.
"""
import argparse
import os
import sys

import pytest


def main():
    parser = argparse.ArgumentParser(description="Run tests for the scrapping pipeline")
    parser.add_argument(
        "--with-coverage", action="store_true", help="Run with coverage"
    )
    parser.add_argument("--junit-xml", help="Path to output JUnit XML report")
    parser.add_argument("--html-report", help="Path to output HTML coverage report")
    args = parser.parse_args()

    # Set environment variables for testing
    os.environ["TESTING"] = "true"
    os.environ["BQ_PROJECT_ID"] = os.environ.get("BQ_PROJECT_ID", "test-project")
    os.environ["BQ_STAGING_DATASET_ID"] = os.environ.get(
        "BQ_STAGING_DATASET_ID", "test_dataset"
    )
    os.environ["BQ_DESTINATION_DETAILS_TABLE_ID"] = os.environ.get(
        "BQ_DESTINATION_DETAILS_TABLE_ID", "test_destinations"
    )
    os.environ["GCS_BUCKET_NAME"] = os.environ.get("GCS_BUCKET_NAME", "test-bucket")
    os.environ["GCS_WEATHER_BUCKET_NAME"] = os.environ.get(
        "GCS_WEATHER_BUCKET_NAME", "test-weather-bucket"
    )

    # Construct pytest arguments
    pytest_args = ["-v"]

    if args.with_coverage:
        pytest_args.extend(
            [
                "--cov=.",
                "--cov-report=term",
            ]
        )
        if args.html_report:
            pytest_args.append(f"--cov-report=html:{args.html_report}")

    if args.junit_xml:
        pytest_args.append(f"--junitxml={args.junit_xml}")

    # Add the current directory to discover tests
    current_dir = os.path.dirname(os.path.abspath(__file__))
    pytest_args.append(current_dir)

    print(f"Running tests with arguments: {pytest_args}")
    result = pytest.main(pytest_args)

    sys.exit(result)


if __name__ == "__main__":
    main()
