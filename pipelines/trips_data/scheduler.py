import time
import logging
import argparse
import schedule
import signal
import sys
from datetime import datetime
from typing import Optional

from . import config
from .pipeline import run_pipeline

# Set up logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('scheduler')

# Flag to control the scheduler loop
running = True

# Default generation method
GENERATION_METHOD = "distribution"

# Authentication options
KEY_PATH = None
PROJECT_ID = None

def signal_handler(sig, frame):
    """Handle termination signals to gracefully stop the scheduler."""
    global running
    logger.info(f"Received signal {sig}, shutting down...")
    running = False

def scheduled_job():
    """Run the pipeline and log the results."""
    logger.info(f"Running scheduled pipeline job at {datetime.now().isoformat()} using {GENERATION_METHOD} method")
    
    # Set the generation method in the pipeline module
    from . import pipeline
    pipeline.GENERATION_METHOD = GENERATION_METHOD
    
    # Run the pipeline with authentication options
    metrics = run_pipeline(
        key_path=KEY_PATH,
        project_id=PROJECT_ID
    )
    
    if metrics['success']:
        logger.info(f"Scheduled job completed successfully. Generated: {metrics['files_generated']} (using {metrics['generation_method']}), Ingested: {metrics['files_ingested']}")
    else:
        logger.error(f"Scheduled job failed: {metrics.get('error', 'Unknown error')}")
    
    return metrics

def run_scheduler(
    time_str: str = "00:00", 
    run_now: bool = False,
    key_path: Optional[str] = None,
    project_id: Optional[str] = None
):
    """Run the scheduler at the specified time each day.
    
    Args:
        time_str: Time to run each day in 24-hour format (HH:MM)
        run_now: Whether to also run immediately
        key_path: Optional path to service account key file
        project_id: Optional Google Cloud project ID
    """
    # Set global authentication options
    global KEY_PATH, PROJECT_ID
    KEY_PATH = key_path
    PROJECT_ID = project_id
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Schedule the job to run at the specified time each day
    schedule.every().day.at(time_str).do(scheduled_job)
    logger.info(f"Scheduled pipeline to run daily at {time_str} using {GENERATION_METHOD} method")
    
    # Run immediately if requested
    if run_now:
        logger.info("Running pipeline now as requested")
        scheduled_job()
    
    # Run the scheduler loop
    while running:
        schedule.run_pending()
        time.sleep(1)
    
    logger.info("Scheduler stopped")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Travel Data Pipeline Scheduler')
    parser.add_argument('--time', default="00:00", 
                        help='Time to run each day (24-hour format, HH:MM)')
    parser.add_argument('--run-now', action='store_true',
                        help='Run the pipeline immediately, then schedule')
    parser.add_argument('--method', choices=['distribution', 'faker'], default='distribution',
                      help='Method to use for data generation')
    parser.add_argument('--key-path',
                        help='Path to service account key file')
    parser.add_argument('--project-id',
                        help='Google Cloud project ID')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    GENERATION_METHOD = args.method
    run_scheduler(
        time_str=args.time, 
        run_now=args.run_now,
        key_path=args.key_path,
        project_id=args.project_id
    ) 