import argparse
import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

from . import config
from .bigquery_ingestion import ingest_files_to_bigquery
from .data_generator import generate_daily_data

# Set up logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("travel_data_pipeline")

# Default generation method
GENERATION_METHOD = "distribution"


def run_pipeline(
    generate: bool = True,
    ingest: bool = True,
    key_path: Optional[str] = None,
    project_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the travel data pipeline.

    Args:
        generate: Whether to generate new data (default: True)
        ingest: Whether to ingest data to BigQuery (default: True)
        key_path: Optional path to a service account key file
        project_id: Optional Google Cloud project ID

    Returns:
        dict: Summary metrics about the pipeline run
    """
    start_time = time.time()
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "files_generated": 0,
        "files_ingested": 0,
        "success": True,
        "generation_method": GENERATION_METHOD,
    }

    try:
        # Step 1: Generate new travel data if requested
        if generate:
            logger.info(
                f"Generating new travel data using {GENERATION_METHOD} method..."
            )

            if GENERATION_METHOD == "faker":
                # Import the faker generator only when needed
                from .faker_data_generator import generate_daily_faker_data

                file_path = generate_daily_faker_data()
            else:
                file_path = generate_daily_data()

            metrics["files_generated"] = 1
            logger.info(f"Generated data file: {file_path}")

        # Step 2: Ingest data to BigQuery if requested
        if ingest:
            logger.info("Ingesting data to BigQuery...")
            files_ingested = ingest_files_to_bigquery(
                key_path=key_path, project_id=project_id
            )
            metrics["files_ingested"] = files_ingested
            logger.info(f"Ingested {files_ingested} files to BigQuery")

    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        metrics["success"] = False
        metrics["error"] = str(e)

    # Calculate execution time
    execution_time = time.time() - start_time
    metrics["execution_time_seconds"] = execution_time

    logger.info(f"Pipeline completed in {execution_time:.2f} seconds")

    return metrics


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Travel Data Pipeline")
    parser.add_argument(
        "--generate-only",
        action="store_true",
        help="Only generate data, skip ingestion",
    )
    parser.add_argument(
        "--ingest-only",
        action="store_true",
        help="Only ingest existing data, skip generation",
    )
    parser.add_argument(
        "--method",
        choices=["distribution", "faker"],
        default="distribution",
        help="Method to use for data generation",
    )
    parser.add_argument("--key-path", help="Path to service account key file")
    parser.add_argument("--project-id", help="Google Cloud project ID")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Determine what to run
    generate = not args.ingest_only
    ingest = not args.generate_only

    # Set the generation method
    GENERATION_METHOD = args.method

    # Run the pipeline
    metrics = run_pipeline(
        generate=generate,
        ingest=ingest,
        key_path=args.key_path,
        project_id=args.project_id,
    )

    # Print a summary
    if metrics["success"]:
        print("Pipeline completed successfully!")
        if generate:
            print(
                f"Files generated: {metrics['files_generated']} (using {metrics['generation_method']} method)"
            )
        if ingest:
            print(f"Files ingested: {metrics['files_ingested']}")
    else:
        print(f"Pipeline failed: {metrics.get('error', 'Unknown error')}")
