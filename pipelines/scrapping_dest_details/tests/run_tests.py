#!/usr/bin/env python
"""
Script to run all tests for the scrapping_dest_details pipeline.
This can be used locally or in CI/CD pipelines.
"""
import argparse
import os
import sys
import pytest
import coverage


def main():
    """
    Run the test suite with or without coverage reporting.
    """
    parser = argparse.ArgumentParser(
        description="Run tests for scrapping_dest_details pipeline"
    )
    parser.add_argument(
        "--with-coverage", action="store_true", help="Run with coverage reporting"
    )
    parser.add_argument("--junit-xml", help="Output JUnit XML report to the given file")
    parser.add_argument(
        "--html-report", help="Output HTML coverage report to the given directory"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Run with verbose output"
    )
    args = parser.parse_args()

    # Change to the directory of this script
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    # Set up pytest arguments
    pytest_args = []

    if args.verbose:
        pytest_args.append("-v")

    if args.junit_xml:
        pytest_args.extend(["--junitxml", args.junit_xml])

    # Add all test files
    pytest_args.extend(
        [
            "test_fetcher.py",
            "test_pipeline.py",
            "test_gcs_storage.py",
            "test_bigquery_loader.py",
            "test_schema_validation.py",
        ]
    )

    if args.with_coverage:
        # Run with coverage
        cov = coverage.Coverage(
            source=["../"],
            omit=[
                "../tests/*",
                "*/__pycache__/*",
                "*/venv/*",
                "*/.venv/*",
            ],
        )
        cov.start()

        exit_code = pytest.main(pytest_args)

        cov.stop()
        cov.save()

        # Print coverage report to console
        print("\nCoverage Report:")
        cov.report()

        # Generate HTML report if requested
        if args.html_report:
            cov.html_report(directory=args.html_report)
            print(f"\nHTML coverage report saved to: {args.html_report}")

    else:
        # Run without coverage
        exit_code = pytest.main(pytest_args)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
